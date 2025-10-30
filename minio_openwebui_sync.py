"""
Airflow DAG for Three-Tier MinIO to OpenWebUI Knowledge Base Sync
- Task 1 (Conditional): hr-knowledgebase → staging (copy new/modified only)
- Task 2 (Always): staging → OpenWebUI (full sync: add/delete/update)
"""

from __future__ import annotations

import os
import time
import requests
import logging
import tempfile
import boto3
import botocore.exceptions
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param, ParamsDict
from airflow.models import Variable

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Use Airflow Variables for tracking
STAGING_TRACKING_VAR = "staging_to_openwebui_tracking"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# ============================================================================
# FILE TRACKING FUNCTIONS
# ============================================================================

def get_tracking_state():
    """Get the current tracking state with staging files mapped to OpenWebUI file IDs."""
    try:
        tracking = Variable.get(STAGING_TRACKING_VAR, default_var={}, deserialize_json=True)
        if not isinstance(tracking, dict):
            return {}
        # Structure: {staging_file_key: {etag, openwebui_file_id, filename, synced_at}}
        return tracking
    except:
        return {}


def update_tracking_state(tracking: dict):
    """Update the tracking state."""
    try:
        Variable.set(STAGING_TRACKING_VAR, tracking, serialize_json=True)
        logger.info(f"✅ Updated tracking: {len(tracking)} files tracked")
    except Exception as e:
        logger.error(f"❌ Error updating tracking: {e}")


# ============================================================================
# MINIO FUNCTIONS
# ============================================================================

def init_s3_client(minio_endpoint, minio_access_key, minio_secret_key, minio_secure):
    """Initialize boto3 S3 client for MinIO."""
    try:
        endpoint_url = f"http{'s' if minio_secure else ''}://{minio_endpoint}"
        
        # Clear proxy settings
        os.environ.pop("HTTP_PROXY", None)
        os.environ.pop("http_proxy", None)
        os.environ.pop("HTTPS_PROXY", None)
        os.environ.pop("https_proxy", None)
        
        client = boto3.client(
            's3',
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            endpoint_url=endpoint_url,
            use_ssl=minio_secure
        )
        
        logger.info(f"✅ S3 client initialized for endpoint: {endpoint_url}")
        return client
        
    except Exception as e:
        logger.error(f"❌ S3 client initialization failed: {e}")
        return None


def scan_minio_bucket(client, bucket: str) -> dict:
    """Scan MinIO bucket and return dict of files with their ETags."""
    try:
        files_dict = {}
        
        # List all objects in bucket
        response = client.list_objects_v2(Bucket=bucket)
        
        if 'Contents' not in response:
            logger.info(f"📭 No files found in bucket: {bucket}")
            return {}
        
        for obj in response['Contents']:
            object_key = obj['Key']
            etag = obj['ETag'].strip('"')
            size_mb = obj['Size'] / (1024 * 1024)
            
            files_dict[object_key] = {
                'etag': etag,
                'size': obj['Size'],
                'size_mb': size_mb,
                'last_modified': obj['LastModified'].isoformat(),
                'filename': os.path.basename(object_key)  # Extract filename
            }
        
        logger.info(f"📊 Scanned {bucket}: found {len(files_dict)} file(s)")
        return files_dict
        
    except botocore.exceptions.ClientError as e:
        logger.error(f"❌ Error scanning MinIO bucket {bucket}: {e}")
        return {}
    except Exception as e:
        logger.error(f"❌ Unexpected error scanning bucket {bucket}: {e}")
        return {}


def copy_file_in_minio(client, source_bucket: str, dest_bucket: str, object_key: str) -> bool:
    """Copy file from one MinIO bucket to another."""
    try:
        copy_source = {'Bucket': source_bucket, 'Key': object_key}
        client.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=object_key)
        logger.info(f"✅ Copied: {object_key} ({source_bucket} → {dest_bucket})")
        return True
    except Exception as e:
        logger.error(f"❌ Error copying {object_key}: {e}")
        return False


def delete_file_from_minio(client, bucket: str, object_key: str) -> bool:
    """Delete file from MinIO bucket."""
    try:
        client.delete_object(Bucket=bucket, Key=object_key)
        logger.info(f"🗑️ Deleted from MinIO: {object_key} (bucket: {bucket})")
        return True
    except Exception as e:
        logger.error(f"❌ Error deleting {object_key} from {bucket}: {e}")
        return False


def download_from_minio(client, object_key: str, bucket: str) -> str:
    """Download file from MinIO to temporary location and return path."""
    try:
        temp_dir = tempfile.mkdtemp()
        filename = os.path.basename(object_key)
        temp_path = os.path.join(temp_dir, filename)
        
        client.download_file(bucket, object_key, temp_path)
        logger.info(f"✅ Downloaded from MinIO: {object_key}")
        return temp_path
        
    except Exception as e:
        logger.error(f"❌ Download failed for {object_key}: {e}")
        return None


# ============================================================================
# OPENWEBUI FUNCTIONS
# ============================================================================

def get_openwebui_knowledge_files(knowledge_id: str, webui_url: str, 
                                   openwebui_api_key: str, verify_ssl: bool) -> dict:
    """Get list of files in OpenWebUI knowledge base. Returns dict keyed by filename."""
    url = f"{webui_url}/api/v1/knowledge/{knowledge_id}"
    headers = {"Authorization": f"Bearer {openwebui_api_key}"}
    
    try:
        response = requests.get(url, headers=headers, verify=verify_ssl, timeout=30)
        if response.status_code == 200:
            data = response.json()
            files_dict = {}
            
            # Extract files from knowledge base data
            files_list = data.get('data', {}).get('files', []) or data.get('files', [])
            
            for file_info in files_list:
                file_id = file_info.get('id')
                filename = file_info.get('filename') or file_info.get('name', 'unknown')
                
                if file_id:
                    # Key by filename (basename) for easier matching
                    files_dict[filename] = {
                        'file_id': file_id,
                        'filename': filename,
                        'data': file_info
                    }
            
            logger.info(f"📊 OpenWebUI knowledge base has {len(files_dict)} file(s)")
            return files_dict
        else:
            logger.error(f"❌ Failed to get OpenWebUI files {response.status_code}: {response.text}")
            return {}
            
    except Exception as e:
        logger.error(f"❌ Error getting OpenWebUI files: {e}")
        return {}


def upload_file_to_webui(file_path: str, webui_url, openwebui_api_key, verify_ssl):
    """Upload file to OpenWebUI and return uploaded_file JSON response."""
    url = f"{webui_url}/api/v1/files/"
    headers = {
        "Authorization": f"Bearer {openwebui_api_key}",
        "Accept": "application/json"
    }

    try:
        with open(file_path, "rb") as f:
            files = {"file": (os.path.basename(file_path), f)}
            response = requests.post(url, headers=headers, files=files, timeout=300, verify=verify_ssl)

        if response.status_code == 200:
            uploaded_file = response.json()
            logger.info(f"✅ File uploaded to WebUI: {uploaded_file.get('id', 'Unknown ID')}")
            return uploaded_file
        else:
            logger.error(f"❌ WebUI upload failed {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"❌ WebUI upload error: {e}")
        return None


def wait_until_processed(file_id: str, file_size_mb: float, webui_url, openwebui_api_key, 
                        verify_ssl, max_wait_time, poll_interval, max_poll_interval) -> bool:
    """Poll file status until it's processed or timeout."""
    url = f"{webui_url}/api/v1/files/{file_id}"
    headers = {"Authorization": f"Bearer {openwebui_api_key}"}

    start_time = time.time()
    attempt = 0
    current_poll_interval = poll_interval
    
    success_statuses = ["success", "completed", "ready", "processed"]
    failed_statuses = ["failed", "error", "cancelled"]
    
    while time.time() - start_time < max_wait_time:
        attempt += 1
        try:
            response = requests.get(url, headers=headers, verify=verify_ssl, timeout=30)
            if response.status_code == 200:
                file_info = response.json()
                status = file_info.get("data", {}).get("status") or file_info.get("status")
                
                logger.info(f"📊 File {file_id} status check #{attempt}: {status}")
                
                if status and status.lower() != "pending":
                    # Wait for metadata extraction
                    metadata_wait_time = min(60 + (file_size_mb * 60), 1800)
                    logger.info(f"⏱️ Waiting {metadata_wait_time}s for metadata extraction...")
                    time.sleep(metadata_wait_time)
                    
                    if status.lower() in success_statuses:
                        logger.info(f"✅ File {file_id} processed successfully")
                        return True
                    elif status.lower() in failed_statuses:
                        logger.error(f"❌ File {file_id} processing failed with status: {status}")
                        return False
                    else:
                        logger.warning(f"⚠️ File {file_id} has unknown status: {status}")
                        return False
                        
        except Exception as e:
            logger.error(f"❌ Error checking file status: {e}")

        time.sleep(current_poll_interval)
        current_poll_interval = min(current_poll_interval * 1.5, max_poll_interval)

    logger.warning(f"⏰ File {file_id} not processed within {max_wait_time} seconds")
    return False


def add_file_to_knowledge(knowledge_id: str, file_id: str, webui_url, openwebui_api_key, verify_ssl):
    """Add file to knowledge base."""
    url = f'{webui_url}/api/v1/knowledge/{knowledge_id}/file/add'
    headers = {
        'Authorization': f'Bearer {openwebui_api_key}',
        'Content-Type': 'application/json'
    }
    data = {'file_id': file_id}
    
    try:
        response = requests.post(url, headers=headers, json=data, verify=verify_ssl, timeout=30)
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ File added to knowledge base")
            return result
        else:
            logger.error(f"❌ Failed to add to knowledge base {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error adding to knowledge base: {e}")
        return None


def remove_file_from_knowledge(knowledge_id: str, file_id: str, webui_url, openwebui_api_key, verify_ssl):
    """Remove file from knowledge base."""
    url = f'{webui_url}/api/v1/knowledge/{knowledge_id}/file/remove'
    headers = {
        'Authorization': f'Bearer {openwebui_api_key}',
        'Content-Type': 'application/json'
    }
    data = {'file_id': file_id}
    
    try:
        response = requests.post(url, headers=headers, json=data, verify=verify_ssl, timeout=30)
        if response.status_code == 200:
            logger.info(f"✅ File removed from knowledge base: {file_id}")
            return True
        else:
            logger.error(f"❌ Failed to remove from knowledge base {response.status_code}: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error removing from knowledge base: {e}")
        return False


# ============================================================================
# TASK 1: HR-KNOWLEDGEBASE → STAGING SYNC
# ============================================================================

def sync_hr_to_staging(**context):
    """
    Task 1: Sync files from hr-knowledgebase to staging
    - Copy NEW files (not in staging)
    - Copy MODIFIED files (ETag changed)
    - NEVER delete from staging
    """
    params = context['params']
    
    # Check if this task should run
    if not params['sync_hr_to_staging']:
        logger.info("⏭️ Skipping hr-knowledgebase → staging sync (sync_hr_to_staging=False)")
        return "skip_hr_sync"
    
    logger.info("🚀 Starting Task 1: hr-knowledgebase → staging sync")
    
    # Initialize S3 client
    s3_client = init_s3_client(
        params['minio_endpoint'],
        params['minio_access_key'],
        params['minio_secret_key'],
        params['minio_secure']
    )
    
    if not s3_client:
        raise ValueError("❌ Failed to initialize S3 client")
    
    # Scan both buckets
    hr_files = scan_minio_bucket(s3_client, params['minio_hr_bucket'])
    staging_files = scan_minio_bucket(s3_client, params['minio_staging_bucket'])
    
    # Calculate what needs to be copied
    to_copy = []
    
    for file_key, file_info in hr_files.items():
        if file_key not in staging_files:
            # New file - needs to be copied
            to_copy.append({
                'key': file_key,
                'reason': 'new',
                'info': file_info
            })
            logger.info(f"📝 NEW file to copy: {file_key}")
        elif staging_files[file_key]['etag'] != file_info['etag']:
            # Modified file - needs to be copied
            to_copy.append({
                'key': file_key,
                'reason': 'modified',
                'info': file_info
            })
            logger.info(f"🔄 MODIFIED file to copy: {file_key}")
    
    if not to_copy:
        logger.info("✅ No new or modified files to copy from hr-knowledgebase to staging")
        return "proceed_to_staging_sync"
    
    # Copy files
    copied_count = 0
    failed_count = 0
    
    for item in to_copy:
        if copy_file_in_minio(
            s3_client,
            params['minio_hr_bucket'],
            params['minio_staging_bucket'],
            item['key']
        ):
            copied_count += 1
        else:
            failed_count += 1
    
    logger.info(f"📊 Task 1 Summary: {copied_count} copied, {failed_count} failed")
    logger.info(f"✅ Task 1 Complete: hr-knowledgebase → staging sync finished")
    
    return "proceed_to_staging_sync"


# ============================================================================
# TASK 2: STAGING → OPENWEBUI SYNC
# ============================================================================

def sync_staging_to_openwebui(**context):
    """
    Task 2: Intelligent delta sync from staging to OpenWebUI
    - Only processes files that changed (added, modified, deleted)
    - Tracks state to avoid re-processing
    - Efficient: 1 file changed = 1 file processed
    """
    params = context['params']
    
    logger.info("🚀 Starting Task 2: staging → OpenWebUI sync (INTELLIGENT DELTA)")
    logger.info(f"🌐 WebUI URL: {params['webui_url']}")
    logger.info(f"🧠 Knowledge ID: {params['knowledge_id']}")
    
    # Validate API key
    if not params['openwebui_api_key'] or params['openwebui_api_key'] == 'sk-xxxxx':
        raise ValueError("❌ Valid OpenWebUI API key is required")
    
    # Initialize S3 client
    s3_client = init_s3_client(
        params['minio_endpoint'],
        params['minio_access_key'],
        params['minio_secret_key'],
        params['minio_secure']
    )
    
    if not s3_client:
        raise ValueError("❌ Failed to initialize S3 client")
    
    # Get current state
    staging_files = scan_minio_bucket(s3_client, params['minio_staging_bucket'])
    openwebui_files = get_openwebui_knowledge_files(
        params['knowledge_id'],
        params['webui_url'],
        params['openwebui_api_key'],
        params['verify_ssl']
    )
    tracking = get_tracking_state()
    
    logger.info(f"📊 Current state: {len(staging_files)} in staging, {len(openwebui_files)} in OpenWebUI, {len(tracking)} tracked")
    
    # Calculate intelligent delta
    to_add = []
    to_update = []
    to_delete = []
    unchanged = []
    
    # Process staging files
    for file_key, file_info in staging_files.items():
        filename = file_info['filename']
        
        # Check if we've already synced this exact version
        if file_key in tracking:
            tracked_info = tracking[file_key]
            
            # If ETag matches and file exists in OpenWebUI, it's unchanged
            if tracked_info.get('etag') == file_info['etag']:
                if filename in openwebui_files:
                    unchanged.append(file_key)
                    logger.debug(f"⏭️ UNCHANGED: {file_key}")
                    continue
                else:
                    # Was synced before but missing from OpenWebUI - re-add
                    logger.warning(f"⚠️ File {file_key} was synced but missing from OpenWebUI - will re-add")
            else:
                # ETag changed - file was modified
                if filename in openwebui_files:
                    to_update.append({
                        'key': file_key,
                        'filename': filename,
                        'info': file_info,
                        'old_file_id': openwebui_files[filename]['file_id']
                    })
                    logger.info(f"🔄 MODIFIED: {file_key}")
                    continue
        
        # New file or not properly tracked
        if filename not in openwebui_files:
            to_add.append({
                'key': file_key,
                'filename': filename,
                'info': file_info
            })
            logger.info(f"➕ NEW: {file_key}")
        else:
            # File exists in OpenWebUI but not tracked properly - check if update needed
            # For safety, mark as update to ensure consistency
            to_update.append({
                'key': file_key,
                'filename': filename,
                'info': file_info,
                'old_file_id': openwebui_files[filename]['file_id']
            })
            logger.info(f"🔄 EXISTS BUT UNTRACKED: {file_key} - will verify/update")
    
    # Find files to delete (in OpenWebUI but not in staging)
    staging_filenames = {info['filename'] for info in staging_files.values()}
    for filename, file_data in openwebui_files.items():
        if filename not in staging_filenames:
            to_delete.append({
                'filename': filename,
                'file_id': file_data['file_id']
            })
            logger.info(f"🗑️ TO DELETE: {filename}")
    
    # Report delta
    logger.info("=" * 60)
    logger.info(f"📋 INTELLIGENT DELTA DETECTED:")
    logger.info(f"   ➕ {len(to_add)} files to ADD")
    logger.info(f"   🔄 {len(to_update)} files to UPDATE")
    logger.info(f"   🗑️ {len(to_delete)} files to DELETE")
    logger.info(f"   ⏭️ {len(unchanged)} files UNCHANGED (skipped)")
    logger.info("=" * 60)
    
    # If nothing to do, exit early
    if not to_add and not to_update and not to_delete:
        logger.info("✅ No changes detected - staging and OpenWebUI are in sync!")
        return
    
    # Execute operations
    added_count = 0
    updated_count = 0
    deleted_count = 0
    failed_count = 0
    
    # Process ADDITIONS
    logger.info(f"➕ Processing {len(to_add)} additions...")
    for item in to_add:
        result = process_file_addition(item, s3_client, params, tracking)
        if result:
            added_count += 1
        else:
            failed_count += 1
    
    # Process UPDATES
    logger.info(f"🔄 Processing {len(to_update)} updates...")
    for item in to_update:
        result = process_file_update(item, s3_client, params, tracking)
        if result:
            updated_count += 1
        else:
            failed_count += 1
    
    # Process DELETIONS
    logger.info(f"🗑️ Processing {len(to_delete)} deletions...")
    for item in to_delete:
        if remove_file_from_knowledge(
            params['knowledge_id'],
            item['file_id'],
            params['webui_url'],
            params['openwebui_api_key'],
            params['verify_ssl']
        ):
            deleted_count += 1
            # Remove from tracking
            keys_to_remove = [k for k, v in tracking.items() if v.get('filename') == item['filename']]
            for key in keys_to_remove:
                del tracking[key]
        else:
            failed_count += 1
    
    # Update tracking state
    update_tracking_state(tracking)
    
    # Final summary
    logger.info("=" * 60)
    logger.info(f"✅ Task 2 Complete: Intelligent delta sync finished")
    logger.info(f"📊 Results:")
    logger.info(f"   ✅ {added_count} added")
    logger.info(f"   ✅ {updated_count} updated")
    logger.info(f"   ✅ {deleted_count} deleted")
    logger.info(f"   ⏭️ {len(unchanged)} skipped (unchanged)")
    logger.info(f"   ❌ {failed_count} failed")
    logger.info("=" * 60)


def process_file_addition(item: dict, s3_client, params, tracking: dict) -> bool:
    """Process adding a new file to OpenWebUI and update tracking."""
    object_key = item['key']
    filename = item['filename']
    file_info = item['info']
    
    try:
        logger.info(f"➕ Adding file: {object_key}")
        
        # Download from staging
        temp_file_path = download_from_minio(s3_client, object_key, params['minio_staging_bucket'])
        if not temp_file_path:
            return False
        
        # Upload to OpenWebUI
        uploaded_file = upload_file_to_webui(
            temp_file_path,
            params['webui_url'],
            params['openwebui_api_key'],
            params['verify_ssl']
        )
        
        if not uploaded_file or "id" not in uploaded_file:
            logger.error(f"❌ Failed to upload: {object_key}")
            cleanup_temp_file(temp_file_path)
            return False
        
        file_id = uploaded_file["id"]
        
        # Wait for processing
        if wait_until_processed(
            file_id,
            file_info['size_mb'],
            params['webui_url'],
            params['openwebui_api_key'],
            params['verify_ssl'],
            params['max_wait_time'],
            params['poll_interval'],
            params['max_poll_interval']
        ):
            # Add to knowledge base
            result = add_file_to_knowledge(
                params['knowledge_id'],
                file_id,
                params['webui_url'],
                params['openwebui_api_key'],
                params['verify_ssl']
            )
            
            if result:
                # Update tracking
                tracking[object_key] = {
                    'etag': file_info['etag'],
                    'openwebui_file_id': file_id,
                    'filename': filename,
                    'synced_at': datetime.now().isoformat()
                }
                logger.info(f"✅ Successfully added and tracked: {object_key}")
                cleanup_temp_file(temp_file_path)
                return True
            else:
                # Check if it's a duplicate error (already exists)
                logger.warning(f"⚠️ Could not add {object_key} - might already exist in KB")
                # Still track it to avoid re-processing
                tracking[object_key] = {
                    'etag': file_info['etag'],
                    'openwebui_file_id': file_id,
                    'filename': filename,
                    'synced_at': datetime.now().isoformat(),
                    'note': 'upload_succeeded_kb_add_failed'
                }
                cleanup_temp_file(temp_file_path)
                return False
        
        logger.error(f"❌ File {object_key} processing timed out")
        cleanup_temp_file(temp_file_path)
        return False
        
    except Exception as e:
        logger.error(f"❌ Error adding file {object_key}: {e}")
        return False


def process_file_update(item: dict, s3_client, params, tracking: dict) -> bool:
    """Process updating an existing file in OpenWebUI."""
    object_key = item['key']
    filename = item['filename']
    old_file_id = item['old_file_id']
    file_info = item['info']
    
    try:
        logger.info(f"🔄 Updating file: {object_key}")
        
        # Remove old version from knowledge base
        if not remove_file_from_knowledge(
            params['knowledge_id'],
            old_file_id,
            params['webui_url'],
            params['openwebui_api_key'],
            params['verify_ssl']
        ):
            logger.warning(f"⚠️ Could not remove old version of {object_key} - will try to add new version anyway")
        
        # Add new version (same as addition process)
        return process_file_addition(item, s3_client, params, tracking)
        
    except Exception as e:
        logger.error(f"❌ Error updating file {object_key}: {e}")
        return False


def cleanup_temp_file(temp_file_path: str):
    """Cleanup temporary file and directory."""
    try:
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
            temp_dir = os.path.dirname(temp_file_path)
            if os.path.exists(temp_dir):
                os.rmdir(temp_dir)
    except:
        pass


# ============================================================================
# AIRFLOW DAG DEFINITION
# ============================================================================

dag = DAG(
    dag_id='minio_openwebui_knowledge_sync',
    default_args=default_args,
    description='Three-tier sync with INTELLIGENT DELTA: hr-knowledgebase → staging → OpenWebUI',
    schedule=None,
    catchup=False,
    tags=['minio', 'openwebui', 'knowledge-base', 'staging', 'three-tier', 'optimized', 'delta-sync'],
    params=ParamsDict(
        {
            # MinIO Configuration
            "minio_endpoint": Param(
                "10.96.1.221:9000",
                type="string",
                description="MinIO endpoint (e.g., 10.96.1.221:9000)",
            ),
            "minio_access_key": Param(
                "admin-io",
                type="string",
                description="MinIO access key",
            ),
            "minio_secret_key": Param(
                "MinIO$2K",
                type="string",
                description="MinIO secret key",
            ),
            "minio_hr_bucket": Param(
                "hr-knowledgebase",
                type="string",
                description="Source bucket (hr-knowledgebase) - all uploaded files",
            ),
            "minio_staging_bucket": Param(
                "hr-staging",
                type="string",
                description="Staging bucket - curated files (source of truth for OpenWebUI)",
            ),
            "minio_secure": Param(
                False,
                type="boolean",
                description="Use HTTPS for MinIO connection",
            ),
            
            # Sync Control
            "sync_hr_to_staging": Param(
                True,
                type="boolean",
                description="Copy new/modified files from hr-knowledgebase to staging (set False when only curating staging)",
            ),
            
            # OpenWebUI Configuration
            "webui_url": Param(
                "http://10.96.3.199:80",
                type="string",
                description="OpenWebUI URL (e.g., http://10.96.3.199:80)",
            ),
            "openwebui_api_key": Param(
                "sk-xxxxx",
                type="string",
                description="OpenWebUI API key for authentication",
            ),
            "knowledge_id": Param(
                "74d89a4c-71e7-43b1-9bf6-43960ddbbd80",
                type="string",
                description="Knowledge base ID inOpenWebUI (e.g., 74d89a4c-71e7-43b1-9bf6-43960ddbbd80)",
            ),
            
            # Connection Settings
            "verify_ssl": Param(
                False,
                type="boolean",
                description="Verify SSL certificates",
            ),
            
            # Timing Configuration
            "max_wait_time": Param(
                3600,
                type="integer",
                description="Maximum wait time for file processing (seconds)",
            ),
            "max_poll_interval": Param(
                900,
                type="integer",
                description="Maximum polling interval (seconds)",
            ),
            "poll_interval": Param(
                5,
                type="integer",
                description="Initial polling interval (seconds)",
            ),
            
            # Logging
            "log_level": Param(
                "INFO",
                type="string",
                enum=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                description="Logging level",
            ),
        }
    ),
    render_template_as_native_obj=True,
    access_control={"All": {"DAGs": {"can_read", "can_edit", "can_delete"}}},
)

# Task 1: HR → Staging (Conditional)
task_hr_to_staging = PythonOperator(
    task_id='sync_hr_to_staging',
    python_callable=sync_hr_to_staging,
    provide_context=True,
    dag=dag,
)

# Task 2: Staging → OpenWebUI (Always runs)
task_staging_to_openwebui = PythonOperator(
    task_id='sync_staging_to_openwebui',
    python_callable=sync_staging_to_openwebui,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
task_hr_to_staging >> task_staging_to_openwebui
