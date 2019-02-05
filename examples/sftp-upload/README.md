# Preparing the Environment

SFTP_USERNAME=<username to login>
SFTP_PASSWORD=<password>
SFTP_KEYFILE=<ssh key file>
SFTP_HOST=<hostname>
SFTP_TARGET=<target directory on sftp server>


# Using flowman

    flowexec -f examples/sftp-upload project run
