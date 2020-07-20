import re
import typing


def get_absolute_import_name(dir_path, import_name):
    return '{}:{}'.format(dir_path, import_name)


def absolute_import_name(import_name):
    return re.fullmatch(r'[\w/]+:[\w/]+', import_name) is not None


def relative_import_name(import_name):
    return not absolute_import_name(import_name)


def split_absolute_import_name(import_name):
    return import_name.split(':')


def get_relative_import_names(import_names):
    return list(name for name in import_names if relative_import_name(name))


def parse_commit_message_targets(commit_message: str) -> typing.List[str]:
    """Parses the import targets from a commit message.

    Import targets are specified by including
    IMPORTS=<comma separated list of import names> in the commit message.

    Args:
        commit_message: GitHub commit message as a string.

    Returns:
        A list of import names each as a string.
    """
    target_lists = re.findall(
        r'(?:IMPORTS=)((?:[\w/]+)(?:,[\w/]+)*)', commit_message)
    targets = set()
    for target_list in target_lists:
        targets.update(target_list.split(','))
    return list(targets)


def import_targetted_by_commit(
        import_dir: str,
        import_name: str,
        import_targets: typing.List[str]) -> bool:
    """Checks if an import should be executed upon the commit.

    See module docstring for the rules.

    Args:
        import_dir: Path to the directory containing the manifest as a string.
        import_name: Name of the import in the manifest as a string.
        import_targets: List of relative and absolute import names each as
            a string parsed from the commit message.

    Returns:
        True, if the import should be executed. False, otherwise.
    """
    absolute_name = get_absolute_import_name(
        import_dir, import_name)
    absolute_all = get_absolute_import_name(import_dir, 'all')
    return ('all' in import_targets or
            absolute_name in import_targets or
            import_name in import_targets or
            absolute_all in import_targets)
