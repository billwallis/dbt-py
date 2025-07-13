YELLOW = "\033[1;33m"
RESET = "\033[0m"


class DbtPyWarning(Warning):
    def __str__(self):
        return f"{DbtPyWarning.__name__}: {super().__str__()}"


# not a fan of the `warnings.warn` output, so just printing directly
def warn(message: str, category: type[Warning]) -> None:
    print(f"{YELLOW}{category(message)}{RESET}")
