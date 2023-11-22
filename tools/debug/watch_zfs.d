#!/usr/sbin/dtrace -qwCs

#pragma D option strsize=16k
#pragma D option switchrate=997hz

#define	PRINT_ARGV(s, i)						\
	if (this->printed) {						\
		this->printed = 0;					\
		if (this->argv[i] != 0) {				\
			this->str = strjoin(s, " ");			\
			this->str = strjoin(s,				\
			    copyinstr(this->argv[i]));			\
			this->printed = 1;				\
		}							\
	}

#define	PRINT_ARGV_ELLIPSIS(s, i)					\
	if (this->printed) {						\
		this->printed = 0;					\
		if (this->argv[i] != 0) {				\
			this->str = strjoin(s, " [...]");		\
			this->printed = 1;				\
		}							\
	}

BEGIN
{
	printf("hunting for zfs(8) errors...\n");

	/*
	 * The pid in the BEGIN action appears to be that of dtrace(8).  Save
	 * it so that we can avoid harrassing any zfs(8) children that we
	 * ourselves create.
	 */
	us = pid;
}

/*
 * NOTE: Take care not to deadlock with our children here!
 */
proc:::exec-success
/execname == "zfs" && !progenyof(us)/
{
	/*
	 * We want to print the full string of the first ~16 arguments.  We
	 * could use "curpsinfo->pr_psargs", but that gets truncated at 80
	 * characters.  Some of our pool names and paths under omicron are
	 * quite a bit longer!
	 *
	 * NOTE: /sbin/zfs is a 64-bit program, otherwise we would have to
	 * check pr_dmodel.
	 */
	this->argv = (userland uint64_t *)curpsinfo->pr_argv;
	this->str = "";
	this->printed = 1;

	/*
	 * funroll loops!
	 */
	PRINT_ARGV(this->str, 0)
	PRINT_ARGV(this->str, 1)
	PRINT_ARGV(this->str, 2)
	PRINT_ARGV(this->str, 3)
	PRINT_ARGV(this->str, 4)
	PRINT_ARGV(this->str, 5)
	PRINT_ARGV(this->str, 6)
	PRINT_ARGV(this->str, 7)
	PRINT_ARGV(this->str, 8)
	PRINT_ARGV(this->str, 9)
	PRINT_ARGV(this->str, 10)
	PRINT_ARGV(this->str, 11)
	PRINT_ARGV(this->str, 12)
	PRINT_ARGV(this->str, 13)
	PRINT_ARGV(this->str, 14)
	PRINT_ARGV(this->str, 15)
	PRINT_ARGV_ELLIPSIS(this->str, 16)

	printf("pid %d: %s\n", pid, this->str);

	/*
	 * Stop the zfs(8) process here, before it gets a chance to do
	 * anything.  Then, start our second D script, pointed directly at that
	 * child process.  The dtrace(8) process will resume the stopped child
	 * automatically as part of grabbing the process (-p).
	 */
	stop();
	system("dtrace -p %d -ZqCws watch_zfs_error.d", pid);
}
