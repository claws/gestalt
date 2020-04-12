import pathlib
import re
import sys
from setuptools import setup, find_packages

assert sys.version_info >= (3, 6, 0), "gestalt requires Python 3.6+"

THIS_DIR = pathlib.Path(__file__).parent


def get_version() -> str:
    init_file = THIS_DIR / "src" / "gestalt" / "__init__.py"
    version_re = re.compile(r".*__version__\s=\s+[\'\"](?P<version>.*?)[\'\"]")
    with open(init_file, "r", encoding="utf8") as init_fd:
        match = version_re.search(init_fd.read())
        if match:
            version = match.group("version")
        else:
            raise RuntimeError(f"Cannot find __version__ in {init_file}")
        return version


def get_long_description() -> str:
    readme_file = THIS_DIR / "README.md"
    with open(readme_file, encoding="utf8") as fd:
        readme = fd.read()
    return readme


def get_requirements(requirements_file: str) -> str:
    with open(requirements_file, encoding="utf8") as fd:
        requirements = []
        for line in fd.read().split("\n"):
            line = line.strip()
            if line and not line.startswith("#"):
                requirements.append(line)
        return requirements


if __name__ == "__main__":
    setup(
        name="gestalt",
        description="gestalt is a Python application framework for building distributed systems",
        long_description=get_long_description(),
        long_description_content_type="text/markdown",
        license="MIT license",
        url="https://github.com/claws/gestalt",
        version=get_version(),
        author="Chris Laws",
        python_requires=">=3.6",
        install_requires=get_requirements(THIS_DIR / "requirements.txt"),
        package_dir={"": "src"},
        packages=find_packages("src"),
        extras_require={
            "develop": get_requirements(THIS_DIR / "requirements.dev.txt"),
            "amq": ["aio_pika"],
            "protobuf": ["protobuf"],
            "yaml": ["PyYAML"],
            "avro": ["avro-python3"],
            "msgpack": ["msgpack-python"],
            "snappy": ["python-snappy"],
            "brotli": ["brotli"],
        },
        classifiers=[
            "Development Status :: 4 - Beta",
            "Intended Audience :: Developers",
            "Programming Language :: Python :: 3.6",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: Implementation :: CPython",
        ],
        keywords=["gestalt", "framework", "communications"],
    )
