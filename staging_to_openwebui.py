"""
 Staging to OpenWebUI Knowledge Base Sync
Intelligently syncs staging bucket to OpenWebUI with precise delta tracking
- Tracks all files synced to OpenWebUI in Airflow Variable
- Only adds NEW files
- Only updates MODIFIED files  
- Only deletes files that were REMOVED from staging
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
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param, ParamsDict
from airflow.models import Variable

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Airflow Variable for tracking synced files
SYNC_TRACKING_VAR = "staging_openwebui_file_mapping"

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
# TRACKING FUNCTIONS
# ============================================================================

def get_sync_tracking():
    """
    Get tracking state: maps staging file paths to OpenWebUI file info
    Structure: {
        "staging_file_path": {
            "etag": "abc123",
            "openwebui_file_id": "uuid",
            "filename": "file.pdf",
            "synced_at": "2025-10-30T10:00:00"
        }
    }
    """
    try:
        tracking = Variable.get(SYNC_TRACKING_VAR, default_var={}, deserialize_json=True)
        if not isinstance(tracking, dict):
            return {}
        logger.info(f"📊 Loaded tracking: {len(tracking)} files currently tracked")
        return tracking
    except Exception as e:
        logger.error(f"❌ Error loading tracking: {e}")
        return {}


def update_sync_tracking(tracking: dict):
    """Update the tracking variable."""
    try:
        Variable.set(SYNC_TRACKING_VAR, tracking, serialize_json=True)
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
                'filename': os.path.basename(object_key)
            }
        
        logger.info(f"📊 Scanned {bucket}: found {len(files_dict)} file(s)")
        return files_dict
        
    except botocore.exceptions.ClientError as e:
        logger.error(f"❌ Error scanning MinIO bucket {bucket}: {e}")
        return {}
    except Exception as e:
        logger.error(f"❌ Unexpected error scanning bucket {bucket}: {e}")
        return {}


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
                    metadata_wait_time = min(60 + (file_size_mb * 60), 1800)
                    logger.info(f"⏱️ Waiting {metadata_wait_time}s for metadata extraction...")
                    time.sleep(metadata_wait_time)
                    
                    if status.lower() in success_statuses:
                        logger.info(f"✅ File {file_id} processed successfully")
                        return True
                    elif status.lower() in failed_statuses:
                        logger.error(f"❌ File {file_id} processing failed")
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
# DELTA CALCULATION
# ============================================================================

def calculate_delta(staging_files: dict, tracking: dict):
    """
    Calculate precise delta by comparing staging bucket with tracking variable.
    Returns: (to_add, to_update, to_delete, unchanged)
    """
    to_add = []
    to_update = []
    to_delete = []
    unchanged = []
    
    # Check each file in staging
    for file_path, file_info in staging_files.items():
        if file_path in tracking:
            # File was previously synced
            tracked_info = tracking[file_path]
            
            if tracked_info.get('etag') == file_info['etag']:
                # Unchanged - skip
                unchanged.append(file_path)
                logger.debug(f"⏭️ UNCHANGED: {file_path}")
            else:
                # ETag changed - file was modified
                to_update.append({
                    'staging_path': file_path,
                    'file_info': file_info,
                    'old_openwebui_file_id': tracked_info.get('openwebui_file_id')
                })
                logger.info(f"🔄 MODIFIED: {file_path}")
        else:
            # New file - not in tracking
            to_add.append({
                'staging_path': file_path,
                'file_info': file_info
            })
            logger.info(f"➕ NEW: {file_path}")
    
    # Find deleted files: in tracking but not in staging
    staging_paths = set(staging_files.keys())
    for tracked_path, tracked_info in tracking.items():
        if tracked_path not in staging_paths:
            to_delete.append({
                'staging_path': tracked_path,
                'filename': tracked_info.get('filename'),
                'openwebui_file_id': tracked_info.get('openwebui_file_id')
            })
            logger.info(f"🗑️ DELETED FROM STAGING: {tracked_path}")
    
    return to_add, to_update, to_delete, unchanged


# ============================================================================
# FILE PROCESSING
# ============================================================================

def process_addition(item: dict, s3_client, params, tracking: dict) -> bool:
    """Add a new file to OpenWebUI and track it."""
    staging_path = item['staging_path']
    file_info = item['file_info']
    filename = file_info['filename']
    
    try:
        logger.info(f"➕ Adding: {staging_path}")
        
        # Download from staging
        temp_path = download_from_minio(s3_client, staging_path, params['minio_staging_bucket'])
        if not temp_path:
            return False
        
        # Upload to OpenWebUI
        uploaded_file = upload_file_to_webui(
            temp_path,
            params['webui_url'],
            params['openwebui_api_key'],
            params['verify_ssl']
        )
        
        if not uploaded_file or "id" not in uploaded_file:
            cleanup_temp_file(temp_path)
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
            
            if result or True:  # Even if KB add fails, track the upload
                # Update tracking
                tracking[staging_path] = {
                    'etag': file_info['etag'],
                    'openwebui_file_id': file_id,
                    'filename': filename,
                    'synced_at': datetime.now().isoformat()
                }
                logger.info(f"✅ Successfully added: {staging_path}")
                cleanup_temp_file(temp_path)
                return True
        
        cleanup_temp_file(temp_path)
        return False
        
    except Exception as e:
        logger.error(f"❌ Error adding {staging_path}: {e}")
        return False


def process_update(item: dict, s3_client, params, tracking: dict) -> bool:
    """Update an existing file in OpenWebUI."""
    staging_path = item['staging_path']
    old_file_id = item['old_openwebui_file_id']
    
    try:
        logger.info(f"🔄 Updating: {staging_path}")
        
        # Remove old version
        if old_file_id:
            remove_file_from_knowledge(
                params['knowledge_id'],
                old_file_id,
                params['webui_url'],
                params['openwebui_api_key'],
                params['verify_ssl']
            )
        
        # Add new version (reuse addition logic)
        return process_addition(item, s3_client, params, tracking)
        
    except Exception as e:
        logger.error(f"❌ Error updating {staging_path}: {e}")
        return False


def process_deletion(item: dict, params, tracking: dict) -> bool:
    """Delete a file from OpenWebUI."""
    staging_path = item['staging_path']
    filename = item['filename']
    file_id = item['openwebui_file_id']
    
    try:
        logger.info(f"🗑️ Deleting: {filename} (was at {staging_path})")
        
        if not file_id:
            logger.warning(f"⚠️ No file_id for {staging_path} - cannot delete")
            # Still remove from tracking
            if staging_path in tracking:
                del tracking[staging_path]
            return False
        
        if remove_file_from_knowledge(
            params['knowledge_id'],
            file_id,
            params['webui_url'],
            params['openwebui_api_key'],
            params['verify_ssl']
        ):
            # Remove from tracking
            if staging_path in tracking:
                del tracking[staging_path]
            logger.info(f"✅ Deleted: {filename}")
            return True
        
        return False
        
    except Exception as e:
        logger.error(f"❌ Error deleting {staging_path}: {e}")
        return False


# ============================================================================
# MAIN SYNC FUNCTION
# ============================================================================

def sync_staging_to_openwebui(**context):
    """
    Intelligent delta sync from staging to OpenWebUI.
    Uses tracking variable to identify exact changes.
    """
    params = context['params']
    
    logger.info("=" * 70)
    logger.info("🚀 STAGING → OPENWEBUI SYNC (DELTA)")
    logger.info("=" * 70)
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
    logger.info(f"📂 Scanning staging bucket: {params['minio_staging_bucket']}")
    staging_files = scan_minio_bucket(s3_client, params['minio_staging_bucket'])
    
    logger.info(f"📋 Loading tracking variable: {SYNC_TRACKING_VAR}")
    tracking = get_sync_tracking()
    
    # Calculate delta
    logger.info("🔍 Calculating delta (what changed)...")
    to_add, to_update, to_delete, unchanged = calculate_delta(staging_files, tracking)
    
    # Report delta
    logger.info("=" * 70)
    logger.info(f"📊 INTELLIGENT DELTA DETECTED:")
    logger.info(f"   ➕ {len(to_add)} NEW files to add")
    logger.info(f"   🔄 {len(to_update)} MODIFIED files to update")
    logger.info(f"   🗑️ {len(to_delete)} DELETED files to remove")
    logger.info(f"   ⏭️ {len(unchanged)} UNCHANGED files (skipped)")
    logger.info("=" * 70)
    
    # Show which files will be deleted
    if to_delete:
        logger.info("🗑️ Files to be DELETED from OpenWebUI:")
        for item in to_delete:
            logger.info(f"   • {item['filename']} (path: {item['staging_path']})")
        logger.info("=" * 70)
    
    # If nothing to do, exit
    if not to_add and not to_update and not to_delete:
        logger.info("✅ No changes detected - staging and OpenWebUI are in sync!")
        logger.info("=" * 70)
        return
    
    # Execute operations
    added_count = 0
    updated_count = 0
    deleted_count = 0
    failed_count = 0
    
    # Process ADDITIONS
    if to_add:
        logger.info(f"➕ Processing {len(to_add)} additions...")
        for item in to_add:
            if process_addition(item, s3_client, params, tracking):
                added_count += 1
            else:
                failed_count += 1
    
    # Process UPDATES
    if to_update:
        logger.info(f"🔄 Processing {len(to_update)} updates...")
        for item in to_update:
            if process_update(item, s3_client, params, tracking):
                updated_count += 1
            else:
                failed_count += 1
    
    # Process DELETIONS
    if to_delete:
        logger.info(f"🗑️ Processing {len(to_delete)} deletions...")
        for item in to_delete:
            if process_deletion(item, params, tracking):
                deleted_count += 1
            else:
                failed_count += 1
    
    # Update tracking
    update_sync_tracking(tracking)
    
    # Final summary
    logger.info("=" * 70)
    logger.info(f"✅ COMPLETE: Intelligent delta sync finished")
    logger.info(f"📊 RESULTS:")
    logger.info(f"   ✅ {added_count} files added")
    logger.info(f"   ✅ {updated_count} files updated")
    logger.info(f"   ✅ {deleted_count} files deleted")
    logger.info(f"   ⏭️ {len(unchanged)} files skipped (unchanged)")
    logger.info(f"   ❌ {failed_count} operations failed")
    logger.info(f"📋 Total files tracked: {len(tracking)}")
    logger.info("=" * 70)


# ============================================================================
# AIRFLOW DAG DEFINITION
# ============================================================================

dag = DAG(
    dag_id='staging_to_openwebui_sync',
    default_args=default_args,
    description=' Intelligent delta sync from staging to OpenWebUI with precise tracking',
    schedule=None,
    catchup=False,
    tags=['minio', 'openwebui', 'knowledge-base', 'staging', 'delta'],
    params=ParamsDict(
        {
            # MinIO Configuration
            "minio_endpoint": Param(
                "10.96.1.221:9000",
                type="string",
                description="MinIO endpoint",
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
            "minio_staging_bucket": Param(
                "hr-staging",
                type="string",
                description="Staging bucket (source of truth)",
            ),
            "minio_secure": Param(
                False,
                type="boolean",
                description="Use HTTPS for MinIO",
            ),
            
            # OpenWebUI Configuration
            "webui_url": Param(
                "http://10.96.3.199:80",
                type="string",
                description="OpenWebUI URL",
            ),
            "openwebui_api_key": Param(
                "sk-xxxxx",
                type="string",
                description="OpenWebUI API key",
            ),
            "knowledge_id": Param(
                "74d89a4c-71e7-43b1-9bf6-43960ddbbd80",
                type="string",
                description="Knowledge base ID",
            ),
            "verify_ssl": Param(
                False,
                type="boolean",
                description="Verify SSL certificates",
            ),
            
            # Timing Configuration
            "max_wait_time": Param(
                3600,
                type="integer",
                description="Max wait time for file processing (seconds)",
            ),
            "max_poll_interval": Param(
                900,
                type="integer",
                description="Max polling interval (seconds)",
            ),
            "poll_interval": Param(
                5,
                type="integer",
                description="Initial polling interval (seconds)",
            ),
        }
    ),
    render_template_as_native_obj=True,
    access_control={"All": {"DAGs": {"can_read", "can_edit", "can_delete"}}},
)

# Single task
sync_task = PythonOperator(
    task_id='sync_staging_to_openwebui',
    python_callable=sync_staging_to_openwebui,
    provide_context=True,
    dag=dag,
)

sync_task