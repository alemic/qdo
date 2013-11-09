# -*- mode: ruby -*-
# vi: set ft=ruby :

$script = <<SCRIPT
  #!/usr/bin/env bash
  set -e

  # Exit if setup is already done (Vagrant 1.2.x backward compatibility).
  if [ -e "/opt/vagrant-installed" ]; then
      exit 0
  fi
  mkdir /opt/vagrant-installed

  export DEBIAN_FRONTEND=noninteractive

  # Date
  # https://help.ubuntu.com/community/UbuntuTime
  TIMEZONE=$(head -n 1 "/etc/timezone")
  echo "Europe/Oslo" | tee /etc/timezone
  dpkg-reconfigure --frontend noninteractive tzdata

  apt-get update -q -y
  apt-get install -q -y python-software-properties git curl

  # Go development environment
  add-apt-repository -y ppa:duh/golang
  apt-get update -q -y
  apt-get install -q -y golang

  # ~/go ~/go/src ~/go/pkg ~/go/bin
  mkdir /home/vagrant/go/pkg
  mkdir /home/vagrant/go/bin
  chown -R vagrant.vagrant /home/vagrant/go

  echo "" >> /home/vagrant/.bashrc
  echo "export GOPATH=~/go" >> /home/vagrant/.bashrc
SCRIPT

Vagrant.configure("2") do |config|
  config.vm.box = "precise64"
  config.vm.box_url = "http://files.vagrantup.com/precise64.box"
  config.vm.network :forwarded_port, guest: 8080, host: 8080
  config.vm.synced_folder ".", "/home/vagrant/go/src/github.com/borgenk/qdo", id: "vagrant-root", :owner => "vagrant", :group => "vagrant"
  config.vm.provision :shell, :inline => $script

  config.vm.provider "virtualbox" do |vb|
    vb.customize ["modifyvm", :id, "--memory", "512"]
    vb.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
    vb.customize ["modifyvm", :id, "--natdnsproxy1", "on"]
  end
end
