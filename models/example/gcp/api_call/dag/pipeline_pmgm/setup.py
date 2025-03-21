import setuptools
REQUIRED_PACKAGES = ['requests==2.26.0', 'workflow', 'google-cloud-storage>=1.42.1', 'google-cloud>=0.34.0',
                     'python-dotenv==1.0.0', 'google-cloud-secret-manager==2.17.0', 'six>=1.10']
PACKAGE_NAME = 'my_package'
PACKAGE_VERSION = '0.0.1'
setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description='Example project',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)
