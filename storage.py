import os
import hashlib
import uuid
from io import BytesIO


class LocalStorage:
    def __init__(self, base_dir=None):
        if base_dir is None:
            base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

    def _resolve(self, key):
        return os.path.join(self.base_dir, key)

    def save(self, file_data, key):
        filepath = self._resolve(key)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        if hasattr(file_data, 'save'):
            file_data.save(filepath)
        else:
            with open(filepath, 'wb') as f:
                f.write(file_data if isinstance(file_data, bytes) else file_data.read())
        return key

    def get_path(self, key):
        filepath = self._resolve(key)
        if os.path.exists(filepath):
            return filepath
        return None

    def exists(self, key):
        return os.path.exists(self._resolve(key))

    def delete(self, key):
        filepath = self._resolve(key)
        if os.path.exists(filepath):
            os.remove(filepath)
            return True
        return False

    def get_url(self, key):
        return None


class OSSStorage:
    def __init__(self, endpoint, bucket, access_key, secret_key, prefix=''):
        self.endpoint = endpoint
        self.bucket_name = bucket
        self.access_key = access_key
        self.secret_key = secret_key
        self.prefix = prefix
        self._bucket = None

    def _get_bucket(self):
        if self._bucket is None:
            import oss2
            auth = oss2.Auth(self.access_key, self.secret_key)
            self._bucket = oss2.Bucket(auth, self.endpoint, self.bucket_name)
        return self._bucket

    def _oss_key(self, key):
        return (self.prefix + '/' + key).lstrip('/')

    def save(self, file_data, key):
        bucket = self._get_bucket()
        oss_key = self._oss_key(key)
        if hasattr(file_data, 'read'):
            bucket.put_object(oss_key, file_data)
        elif hasattr(file_data, 'save'):
            data = BytesIO()
            file_data.save(data)
            data.seek(0)
            bucket.put_object(oss_key, data)
        else:
            bucket.put_object(oss_key, file_data)
        return key

    def get_path(self, key):
        return None

    def exists(self, key):
        try:
            bucket = self._get_bucket()
            return bucket.object_exists(self._oss_key(key))
        except Exception:
            return False

    def delete(self, key):
        try:
            bucket = self._get_bucket()
            bucket.delete_object(self._oss_key(key))
            return True
        except Exception:
            return False

    def get_url(self, key, expires=3600):
        try:
            bucket = self._get_bucket()
            return bucket.sign_url('GET', self._oss_key(key), expires)
        except Exception:
            return None



def get_storage():
    storage_type = os.environ.get('STORAGE_TYPE', 'local')
    if storage_type == 'oss':
        endpoint = os.environ.get('OSS_ENDPOINT', '')
        bucket = os.environ.get('OSS_BUCKET', '')
        access_key = os.environ.get('OSS_ACCESS_KEY', '')
        secret_key = os.environ.get('OSS_SECRET_KEY', '')
        prefix = os.environ.get('OSS_PREFIX', 'club-stats')
        if endpoint and bucket and access_key and secret_key:
            return OSSStorage(endpoint, bucket, access_key, secret_key, prefix)
    return LocalStorage()


def migrate_old_paths(db_conn):
    storage = get_storage()
    if not isinstance(storage, LocalStorage):
        return 0
    data_dir = storage.base_dir
    tables = [
        ('club_uploads', 'file_path', 'club_uploads'),
        ('offcampus_requests', 'file_path', 'offcampus'),
        ('finance_records', 'attachment_path', 'finance_attachments'),
        ('notices', 'attachment_path', 'notices'),
        ('feedbacks', 'file_path', 'feedback_files'),
        ('checkin_sessions', 'plan_path', 'activity_plans'),
    ]
    migrated = 0
    for table, col, prefix in tables:
        try:
            rows = db_conn.execute(f'SELECT id, {col} FROM {table} WHERE {col} IS NOT NULL AND {col} != ""').fetchall()
        except Exception:
            continue
        for row in rows:
            old_path = (row[col] or '').strip()
            if not old_path:
                continue
            if old_path.startswith(prefix + '/') or old_path.startswith(prefix + '\\'):
                continue
            src = old_path
            if not os.path.isabs(src):
                src = os.path.join(data_dir, src.replace('\\', '/').lstrip('/'))
            if not os.path.exists(src):
                continue
            filename = os.path.basename(src)
            new_key = prefix + '/' + filename
            new_path = storage._resolve(new_key)
            try:
                os.makedirs(os.path.dirname(new_path), exist_ok=True)
                import shutil
                shutil.move(src, new_path)
                db_conn.execute(f'UPDATE {table} SET {col}=? WHERE id=?', (new_key, row['id']))
                migrated += 1
            except Exception:
                pass
    if migrated:
        db_conn.commit()
    return migrated


storage = get_storage()