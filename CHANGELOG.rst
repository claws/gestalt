Change Log
==========

Notable changes to this project will be documented in this file.

Version History
---------------

20.X.X
++++++

- Updated CI to use Github actions
- Remove reuse_address argument from UDP endpoints. This feature was removed in Python 3.8 due to security concerns.

20.1.1
++++++

- Update package to support Python3.8
- Use yarl package for URLs.
- Add linting to improve code sustainment.
- Fix bug in stream protocols that affected msg_len in scenarios where messages were fragmented.

19.9.2
++++++

- Fix markdown displayed on PyPI.

19.9.1
++++++

- Adopted CalVer for package versioning.
- Initial functionality release.
- Clean up type annotation to remove all Mypy errors.
- Add Mypy type check to CI.
- Improve unit test code coverage.

0.2.0
++++++

- Added basic functionality.

0.0.1
++++++

- Project created.
