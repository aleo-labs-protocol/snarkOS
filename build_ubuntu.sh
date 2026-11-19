#!/bin/bash
echo "================================================"
echo " Attention - Building snarkOS from source code."
echo " This will request root permissions with sudo,"
echo " to install dependencies"
echo "================================================"

# Install Ubuntu dependencies

sudo apt-get update
sudo apt-get install -y \
	build-essential \
	curl \
	clang \
	gcc \
	jq \
	libssl-dev \
	llvm \
	lld \
	make \
	pkg-config \
	tmux \
	xz-utils \
	ufw


# Install Rust

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env

# Install snarkOS
# cargo clean
cargo install --locked --path .

echo "=================================================="
echo " Attention - Please ensure ports 4130 and 3030"
echo "             are enabled on your local network."
echo ""
echo " Cloud Providers - Enable ports 4130 and 3030"
echo "                   in your network firewall"
echo ""
echo " Home Users - Enable port forwarding or NAT rules"
echo "              for 4130 and 3030 on your router."
echo "=================================================="
