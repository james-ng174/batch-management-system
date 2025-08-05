#!/bin/bash

# Define the Miniconda installer URL
CONDA_URL="https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh"

# Define the installation path
INSTALL_PATH="$HOME/miniconda3"

# Download the Miniconda installer
echo "Downloading Miniconda installer..."
curl -o Miniconda3-latest-Linux-x86_64.sh $CONDA_URL

# Verify the installer (optional, you can remove this part if you don't need it)
echo "Verifying installer checksum..."
SHA256SUM=$(sha256sum Miniconda3-latest-Linux-x86_64.sh | awk '{print $1}')
EXPECTED_SUM=$(curl -s https://repo.anaconda.com/miniconda/sha256sum.txt | grep Miniconda3-latest-Linux-x86_64.sh | awk '{print $1}')

if [ "$SHA256SUM" != "$EXPECTED_SUM" ]; then
  echo "Checksum verification failed! Exiting."
  exit 1
fi

# Run the installer
echo "Running the Miniconda installer..."
bash Miniconda3-latest-Linux-x86_64.sh -b -p $INSTALL_PATH

# Initialize conda
echo "Initializing conda..."
$INSTALL_PATH/bin/conda init

# Cleanup
echo "Cleaning up..."
rm Miniconda3-latest-Linux-x86_64.sh

# Source the .bashrc to activate conda
echo "Sourcing .bashrc to activate conda..."
source ~/.bashrc

# Display the conda version to verify installation
echo "Verifying conda installation..."
conda --version

echo "Conda installation completed successfully!"
