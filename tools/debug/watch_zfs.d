#!/usr/sbin/dtrace -s

#pragma D option switchrate=997hz

BEGIN
{
	printf("hunting for zfs(8) errors...\n");
}

proc:::exec-success
/execname == "zfs"/
{
	printf("pid %d is zfs\n", pid);
	stop();
	system("dtrace -p %d -Zqws watch_zfs_error.d", pid);
}
