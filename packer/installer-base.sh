#!/bin/bash -xe
# check if it runs as root
[ "$(id -u)" != "0" ] && echo "ERROR: The script needs to be executed as root user.." && exit 1


apt-get update
systemctl disable systemd-resolved
systemctl stop systemd-resolved

apt-get install -y dnsmasq

cp /etc/dhcp/dhclient.conf /etc/dhcp/dhclient.conf.orig
echo 'supersede domain-name-servers 127.0.0.1, 8.8.8.8;' | sudo tee -a /etc/dhcp/dhclient.conf
## In debian dhclient is started by networking.service and/or ifup@eth0.service,
## for now restarting networking
systemctl restart networking


############## Install Docker ##############
apt-get install -y \
     apt-transport-https \
     ca-certificates \
     curl \
     gnupg2 \
     software-properties-common

curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -

add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/debian \
   $(lsb_release -cs) \
   stable"

apt-get update

apt-get install -y docker-ce=5:18.09.7~3-0~debian-stretch

usermod -aG docker admin

exit 0
