import os
import shutil
import ssl
import subprocess
import urllib.request
import requests
from functools import lru_cache
from typing import List, Tuple
from urllib.parse import urlparse
import hashlib

from tqdm import tqdm

from facefusion import logger, process_manager, state_manager, wording
from facefusion.common_helper import is_macos
from facefusion.filesystem import get_file_size, is_file, remove_file
from facefusion.hash_helper import validate_hash
from facefusion.typing import DownloadSet

if is_macos():
	ssl._create_default_https_context = ssl._create_unverified_context


def conditional_download(download_directory_path: str, urls: List[str], max_retries: int = 3) -> None:
	for url in urls:
		download_file_name = os.path.basename(urlparse(url).path)
		download_file_path = os.path.join(download_directory_path, download_file_name)
		initial_size = get_file_size(download_file_path)
		download_size = get_download_size(url)

		if initial_size < download_size:
			for attempt in range(max_retries):
				try:
					with requests.get(url, stream=True) as response:
						response.raise_for_status()
						total_size = int(response.headers.get('content-length', 0))

						with open(download_file_path, 'wb') as file, tqdm(
							desc=wording.get('downloading'),
							total=total_size,
							unit='iB',
							unit_scale=True,
							unit_divisor=1024,
						) as progress_bar:
							for data in response.iter_content(chunk_size=8192):
								size = file.write(data)
								progress_bar.update(size)

					# Verify checksum here if available

					break  # Successful download, exit retry loop
				except requests.RequestException as e:
					logger.error(f"Download failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
					if attempt == max_retries - 1:
						logger.error(f"Failed to download {url} after {max_retries} attempts.")
						return False
	return True


@lru_cache(maxsize = None)
def get_download_size(url : str) -> int:
	try:
		response = urllib.request.urlopen(url, timeout = 10)
		content_length = response.headers.get('Content-Length')
		return int(content_length)
	except (OSError, TypeError, ValueError):
		return 0


def is_download_done(url : str, file_path : str) -> bool:
	if is_file(file_path):
		return get_download_size(url) == get_file_size(file_path)
	return False


def conditional_download_hashes(download_directory_path : str, hashes : DownloadSet) -> bool:
	hash_paths = [ hashes.get(hash_key).get('path') for hash_key in hashes.keys() ]

	process_manager.check()
	if not state_manager.get_item('skip_download'):
		_, invalid_hash_paths = validate_hash_paths(hash_paths)
		if invalid_hash_paths:
			for index in hashes:
				if hashes.get(index).get('path') in invalid_hash_paths:
					invalid_hash_url = hashes.get(index).get('url')
					conditional_download(download_directory_path, [ invalid_hash_url ])

	valid_hash_paths, invalid_hash_paths = validate_hash_paths(hash_paths)
	for valid_hash_path in valid_hash_paths:
		valid_hash_file_name, _ = os.path.splitext(os.path.basename(valid_hash_path))
		logger.debug(wording.get('validating_hash_succeed').format(hash_file_name = valid_hash_file_name), __name__)
	for invalid_hash_path in invalid_hash_paths:
		invalid_hash_file_name, _ = os.path.splitext(os.path.basename(invalid_hash_path))
		logger.error(wording.get('validating_hash_failed').format(hash_file_name = invalid_hash_file_name), __name__)

	if not invalid_hash_paths:
		process_manager.end()
	return not invalid_hash_paths


def conditional_download_sources(download_directory_path : str, sources : DownloadSet) -> bool:
	source_paths = [ sources.get(source_key).get('path') for source_key in sources.keys() ]

	process_manager.check()
	if not state_manager.get_item('skip_download'):
		_, invalid_source_paths = validate_source_paths(source_paths)
		if invalid_source_paths:
			for index in sources:
				if sources.get(index).get('path') in invalid_source_paths:
					invalid_source_url = sources.get(index).get('url')
					conditional_download(download_directory_path, [ invalid_source_url ])

	valid_source_paths, invalid_source_paths = validate_source_paths(source_paths)
	for valid_source_path in valid_source_paths:
		valid_source_file_name, _ = os.path.splitext(os.path.basename(valid_source_path))
		logger.debug(wording.get('validating_source_succeed').format(source_file_name = valid_source_file_name), __name__)
	for invalid_source_path in invalid_source_paths:
		invalid_source_file_name, _ = os.path.splitext(os.path.basename(invalid_source_path))
		logger.error(wording.get('validating_source_failed').format(source_file_name = invalid_source_file_name), __name__)

		if remove_file(invalid_source_path):
			logger.error(wording.get('deleting_corrupt_source').format(source_file_name = invalid_source_file_name), __name__)

	if not invalid_source_paths:
		process_manager.end()
	return not invalid_source_paths


def validate_hash_paths(hash_paths : List[str]) -> Tuple[List[str], List[str]]:
	valid_hash_paths = []
	invalid_hash_paths = []

	for hash_path in hash_paths:
		if is_file(hash_path):
			valid_hash_paths.append(hash_path)
		else:
			invalid_hash_paths.append(hash_path)
	return valid_hash_paths, invalid_hash_paths


def validate_source_paths(source_paths : List[str]) -> Tuple[List[str], List[str]]:
	valid_source_paths = []
	invalid_source_paths = []

	for source_path in source_paths:
		if validate_hash(source_path):
			valid_source_paths.append(source_path)
		else:
			invalid_source_paths.append(source_path)
	return valid_source_paths, invalid_source_paths
