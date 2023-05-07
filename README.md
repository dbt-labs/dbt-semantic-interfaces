<p align="center">
    <a target="_blank" href="https://twitter.com/dbt_labs">
    <img src="https://img.shields.io/twitter/follow/dbt_labs?labelColor=image.png&color=163B36&logo=twitter&style=flat">
  </a>
    <a target="_blank" href="https://www.getdbt.com/community/">
    <img src="https://img.shields.io/badge/Slack-join-163B36">
  </a>
    <a href="https://github.com/psf/black"><img src="https://img.shields.io/badge/code%20style-black-000000.svg" /></a>
</p>

# dbt-semantic-interfaces

This repo contains the shared semantic classes, default validation, and tests designed to be used by both the dbt-core and MetricFlow projects. By centralizing these shared resources, we aim to maintain consistency and reduce code duplication across both projects. 

## Features
- Protocols for shared semantic classes: Define the interfaces and common attributes that must be implemented by the objects in both projects.
- Validation: Ensure that the objects comply with the expected structure and constraints.
- Tests: Ensure that the objects' behavior is consistent and correct across both projects.

## Contributing
We welcome contributions to improve this codebase! If you're interested in contributing, please read our [contributing guidelines](CONTRIBUTING.md) and [code of conduct](CODE_OF_CONDUCT.md) first.

## License
This package is released under the Apache2 License.

## Support
If you encounter any issues or have questions regarding the repo, please open an issue!
