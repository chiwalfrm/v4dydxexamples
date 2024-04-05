#!/bin/sh
if [ `whoami` != root ]
then
        sudostring=sudo
fi
$sudostring mount -t tmpfs -o rw,size=1536m tmpfs /mnt/ramdisk5
$sudostring mount -t tmpfs -o rw,size=7G tmpfs /mnt/ramdisk6
