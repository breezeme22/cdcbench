from cx_Freeze import setup, Executable

build_options = {
    "packages": ["sqlalchemy", "cx_Oracle"],
    "include_files": ["config.ini"]
}

setup(
    name = "cdcbench",
    version = "1.0",
    description = "Oracle Simple benchmarking test tool.",
    author = "Sangcheol, Park",
    options = {"build_exe": build_options},
    executables = [Executable("main.py")]
)