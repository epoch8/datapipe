import sys
import importlib


def modify_and_import_code():
    spec = importlib.util.find_spec('label_studio.project')
    source = spec.loader.get_source('label_studio.project')
    new_source = source.replace(
        "return ['completions-dir', 's3-completions', 'gcs-completions']",
        "return ['completions-dir', 's3-completions', 'gcs-completions', 'completions-dir-modified']",
    )
    module = importlib.util.module_from_spec(spec)
    codeobj = compile(new_source, module.__spec__.origin, 'exec')
    exec(codeobj, module.__dict__)
    sys.modules['label_studio.project'] = module
    return module


def main():
    # Fix logging problems (they appear cuz of logging.config.dictConfig disabling active loggers)
    # ---
    from label_studio.server import setup_default_logging_config, check_for_the_latest_version
    setup_default_logging_config()
    check_for_the_latest_version()
    from label_studio.blueprint import main as server_run  # noqa: F401
    # ---

    modify_and_import_code()

    from label_studio.storage.base import register_storage
    from c12n_pipe.label_studio_utils.filesystem_modified import CompletionsDirStorageModified
    register_storage('completions-dir-modified', CompletionsDirStorageModified)

    if '--target' not in sys.argv and '--target_path' not in sys.argv:
        sys.argv.extend(['--target', 'completions-dir-modified', '--target-path', 'completions'])

    server_run()


if __name__ == "__main__":
    sys.exit(main())
