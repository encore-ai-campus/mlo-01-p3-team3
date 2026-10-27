"""Read existing service-owned Java properties without exporting their secrets."""
import os
import stat

from .errors import PipelineError


def service_credentials(path):
    """Only the current service identity may use its private, regular file."""
    if os.name != 'posix':
        raise PipelineError('S3_SERVICE_CREDENTIALS_POSIX_REQUIRED')
    descriptor = None
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size > 16384):
            raise PipelineError('S3_SERVICE_CREDENTIALS_PERMISSIONS')
        with os.fdopen(descriptor, encoding='utf-8') as stream:
            descriptor = None
            content = stream.read(16385)
        if len(content.encode('utf-8')) > 16384:
            raise PipelineError('S3_SERVICE_CREDENTIALS_FORMAT')
        values = {}
        for line in content.splitlines():
            if not line.strip() or line.lstrip().startswith(('#', '!')):
                continue
            key, separator, value = line.partition('=')
            key, value = key.strip(), value.strip()
            if key not in {'accessKey', 'secretKey'}:
                continue
            if not separator or not value or key in values or '\\' in value:
                raise PipelineError('S3_SERVICE_CREDENTIALS_FORMAT')
            values[key] = value
        if set(values) != {'accessKey', 'secretKey'}:
            raise PipelineError('S3_SERVICE_CREDENTIALS_FORMAT')
        return dict(aws_access_key_id=values['accessKey'], aws_secret_access_key=values['secretKey'])
    except PipelineError:
        raise
    except Exception:
        raise PipelineError('S3_SERVICE_CREDENTIALS_UNREADABLE') from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
