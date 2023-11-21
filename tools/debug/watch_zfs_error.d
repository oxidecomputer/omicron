#!/usr/sbin/dtrace -s

#pragma D option switchrate=997hz

enum {
	EZFS_SUCCESS = 0,	/* no error -- success */
	EZFS_NOMEM = 2000,	/* out of memory */
	EZFS_BADPROP,		/* invalid property value */
	EZFS_PROPREADONLY,	/* cannot set readonly property */
	EZFS_PROPTYPE,		/* property does not apply to dataset type */
	EZFS_PROPNONINHERIT,	/* property is not inheritable */
	EZFS_PROPSPACE,		/* bad quota or reservation */
	EZFS_BADTYPE,		/* dataset is not of appropriate type */
	EZFS_BUSY,		/* pool or dataset is busy */
	EZFS_EXISTS,		/* pool or dataset already exists */
	EZFS_NOENT,		/* no such pool or dataset */
	EZFS_BADSTREAM,		/* bad backup stream */
	EZFS_DSREADONLY,	/* dataset is readonly */
	EZFS_VOLTOOBIG,		/* volume is too large for 32-bit system */
	EZFS_INVALIDNAME,	/* invalid dataset name */
	EZFS_BADRESTORE,	/* unable to restore to destination */
	EZFS_BADBACKUP,		/* backup failed */
	EZFS_BADTARGET,		/* bad attach/detach/replace target */
	EZFS_NODEVICE,		/* no such device in pool */
	EZFS_BADDEV,		/* invalid device to add */
	EZFS_NOREPLICAS,	/* no valid replicas */
	EZFS_RESILVERING,	/* currently resilvering */
	EZFS_BADVERSION,	/* unsupported version */
	EZFS_POOLUNAVAIL,	/* pool is currently unavailable */
	EZFS_DEVOVERFLOW,	/* too many devices in one vdev */
	EZFS_BADPATH,		/* must be an absolute path */
	EZFS_CROSSTARGET,	/* rename or clone across pool or dataset */
	EZFS_ZONED,		/* used improperly in local zone */
	EZFS_MOUNTFAILED,	/* failed to mount dataset */
	EZFS_UMOUNTFAILED,	/* failed to unmount dataset */
	EZFS_UNSHARENFSFAILED,	/* unshare(8) failed */
	EZFS_SHARENFSFAILED,	/* share(8) failed */
	EZFS_PERM,		/* permission denied */
	EZFS_NOSPC,		/* out of space */
	EZFS_FAULT,		/* bad address */
	EZFS_IO,		/* I/O error */
	EZFS_INTR,		/* signal received */
	EZFS_ISSPARE,		/* device is a hot spare */
	EZFS_INVALCONFIG,	/* invalid vdev configuration */
	EZFS_RECURSIVE,		/* recursive dependency */
	EZFS_NOHISTORY,		/* no history object */
	EZFS_POOLPROPS,		/* couldn't retrieve pool props */
	EZFS_POOL_NOTSUP,	/* ops not supported for this type of pool */
	EZFS_POOL_INVALARG,	/* invalid argument for this pool operation */
	EZFS_NAMETOOLONG,	/* dataset name is too long */
	EZFS_OPENFAILED,	/* open of device failed */
	EZFS_NOCAP,		/* couldn't get capacity */
	EZFS_LABELFAILED,	/* write of label failed */
	EZFS_BADWHO,		/* invalid permission who */
	EZFS_BADPERM,		/* invalid permission */
	EZFS_BADPERMSET,	/* invalid permission set name */
	EZFS_NODELEGATION,	/* delegated administration is disabled */
	EZFS_UNSHARESMBFAILED,	/* failed to unshare over smb */
	EZFS_SHARESMBFAILED,	/* failed to share over smb */
	EZFS_BADCACHE,		/* bad cache file */
	EZFS_ISL2CACHE,		/* device is for the level 2 ARC */
	EZFS_VDEVNOTSUP,	/* unsupported vdev type */
	EZFS_NOTSUP,		/* ops not supported on this dataset */
	EZFS_ACTIVE_SPARE,	/* pool has active shared spare devices */
	EZFS_UNPLAYED_LOGS,	/* log device has unplayed logs */
	EZFS_REFTAG_RELE,	/* snapshot release: tag not found */
	EZFS_REFTAG_HOLD,	/* snapshot hold: tag already exists */
	EZFS_TAGTOOLONG,	/* snapshot hold/rele: tag too long */
	EZFS_PIPEFAILED,	/* pipe create failed */
	EZFS_THREADCREATEFAILED, /* thread create failed */
	EZFS_POSTSPLIT_ONLINE,	/* onlining a disk after splitting it */
	EZFS_SCRUBBING,		/* currently scrubbing */
	EZFS_NO_SCRUB,		/* no active scrub */
	EZFS_DIFF,		/* general failure of zfs diff */
	EZFS_DIFFDATA,		/* bad zfs diff data */
	EZFS_POOLREADONLY,	/* pool is in read-only mode */
	EZFS_SCRUB_PAUSED,	/* scrub currently paused */
	EZFS_ACTIVE_POOL,	/* pool is imported on a different system */
	EZFS_CRYPTOFAILED,	/* failed to setup encryption */
	EZFS_NO_PENDING,	/* cannot cancel, no operation is pending */
	EZFS_CHECKPOINT_EXISTS,	/* checkpoint exists */
	EZFS_DISCARDING_CHECKPOINT,	/* currently discarding a checkpoint */
	EZFS_NO_CHECKPOINT,	/* pool has no checkpoint */
	EZFS_DEVRM_IN_PROGRESS,	/* a device is currently being removed */
	EZFS_VDEV_TOO_BIG,	/* a device is too big to be used */
	EZFS_TOOMANY,		/* argument list too long */
	EZFS_INITIALIZING,	/* currently initializing */
	EZFS_NO_INITIALIZE,	/* no active initialize */
	EZFS_WRONG_PARENT,	/* invalid parent dataset (e.g ZVOL) */
	EZFS_TRIMMING,		/* currently trimming */
	EZFS_NO_TRIM,		/* no active trim */
	EZFS_TRIM_NOTSUP,	/* device does not support trim */
	EZFS_NO_RESILVER_DEFER,	/* pool doesn't support resilver_defer */
	EZFS_IOC_NOTSUPPORTED,	/* operation not supported by zfs module */
	EZFS_UNKNOWN
};

inline string enam[int r] =
	r == EZFS_SUCCESS ? "EZFS_SUCCESS" :
	r == EZFS_NOMEM ? "EZFS_NOMEM" :
	r == EZFS_BADPROP ? "EZFS_BADPROP" :
	r == EZFS_PROPREADONLY ? "EZFS_PROPREADONLY" :
	r == EZFS_PROPTYPE ? "EZFS_PROPTYPE" :
	r == EZFS_PROPNONINHERIT ? "EZFS_PROPNONINHERIT" :
	r == EZFS_PROPSPACE ? "EZFS_PROPSPACE" :
	r == EZFS_BADTYPE ? "EZFS_BADTYPE" :
	r == EZFS_BUSY ? "EZFS_BUSY" :
	r == EZFS_EXISTS ? "EZFS_EXISTS" :
	r == EZFS_NOENT ? "EZFS_NOENT" :
	r == EZFS_BADSTREAM ? "EZFS_BADSTREAM" :
	r == EZFS_DSREADONLY ? "EZFS_DSREADONLY" :
	r == EZFS_VOLTOOBIG ? "EZFS_VOLTOOBIG" :
	r == EZFS_INVALIDNAME ? "EZFS_INVALIDNAME" :
	r == EZFS_BADRESTORE ? "EZFS_BADRESTORE" :
	r == EZFS_BADBACKUP ? "EZFS_BADBACKUP" :
	r == EZFS_BADTARGET ? "EZFS_BADTARGET" :
	r == EZFS_NODEVICE ? "EZFS_NODEVICE" :
	r == EZFS_BADDEV ? "EZFS_BADDEV" :
	r == EZFS_NOREPLICAS ? "EZFS_NOREPLICAS" :
	r == EZFS_RESILVERING ? "EZFS_RESILVERING" :
	r == EZFS_BADVERSION ? "EZFS_BADVERSION" :
	r == EZFS_POOLUNAVAIL ? "EZFS_POOLUNAVAIL" :
	r == EZFS_DEVOVERFLOW ? "EZFS_DEVOVERFLOW" :
	r == EZFS_BADPATH ? "EZFS_BADPATH" :
	r == EZFS_CROSSTARGET ? "EZFS_CROSSTARGET" :
	r == EZFS_ZONED ? "EZFS_ZONED" :
	r == EZFS_MOUNTFAILED ? "EZFS_MOUNTFAILED" :
	r == EZFS_UMOUNTFAILED ? "EZFS_UMOUNTFAILED" :
	r == EZFS_UNSHARENFSFAILED ? "EZFS_UNSHARENFSFAILED" :
	r == EZFS_SHARENFSFAILED ? "EZFS_SHARENFSFAILED" :
	r == EZFS_PERM ? "EZFS_PERM" :
	r == EZFS_NOSPC ? "EZFS_NOSPC" :
	r == EZFS_FAULT ? "EZFS_FAULT" :
	r == EZFS_IO ? "EZFS_IO" :
	r == EZFS_INTR ? "EZFS_INTR" :
	r == EZFS_ISSPARE ? "EZFS_ISSPARE" :
	r == EZFS_INVALCONFIG ? "EZFS_INVALCONFIG" :
	r == EZFS_RECURSIVE ? "EZFS_RECURSIVE" :
	r == EZFS_NOHISTORY ? "EZFS_NOHISTORY" :
	r == EZFS_POOLPROPS ? "EZFS_POOLPROPS" :
	r == EZFS_POOL_NOTSUP ? "EZFS_POOL_NOTSUP" :
	r == EZFS_POOL_INVALARG ? "EZFS_POOL_INVALARG" :
	r == EZFS_NAMETOOLONG ? "EZFS_NAMETOOLONG" :
	r == EZFS_OPENFAILED ? "EZFS_OPENFAILED" :
	r == EZFS_NOCAP ? "EZFS_NOCAP" :
	r == EZFS_LABELFAILED ? "EZFS_LABELFAILED" :
	r == EZFS_BADWHO ? "EZFS_BADWHO" :
	r == EZFS_BADPERM ? "EZFS_BADPERM" :
	r == EZFS_BADPERMSET ? "EZFS_BADPERMSET" :
	r == EZFS_NODELEGATION ? "EZFS_NODELEGATION" :
	r == EZFS_UNSHARESMBFAILED ? "EZFS_UNSHARESMBFAILED" :
	r == EZFS_SHARESMBFAILED ? "EZFS_SHARESMBFAILED" :
	r == EZFS_BADCACHE ? "EZFS_BADCACHE" :
	r == EZFS_ISL2CACHE ? "EZFS_ISL2CACHE" :
	r == EZFS_VDEVNOTSUP ? "EZFS_VDEVNOTSUP" :
	r == EZFS_NOTSUP ? "EZFS_NOTSUP" :
	r == EZFS_ACTIVE_SPARE ? "EZFS_ACTIVE_SPARE" :
	r == EZFS_UNPLAYED_LOGS ? "EZFS_UNPLAYED_LOGS" :
	r == EZFS_REFTAG_RELE ? "EZFS_REFTAG_RELE" :
	r == EZFS_REFTAG_HOLD ? "EZFS_REFTAG_HOLD" :
	r == EZFS_TAGTOOLONG ? "EZFS_TAGTOOLONG" :
	r == EZFS_PIPEFAILED ? "EZFS_PIPEFAILED" :
	r == EZFS_THREADCREATEFAILED ? "EZFS_THREADCREATEFAILED" :
	r == EZFS_POSTSPLIT_ONLINE ? "EZFS_POSTSPLIT_ONLINE" :
	r == EZFS_SCRUBBING ? "EZFS_SCRUBBING" :
	r == EZFS_NO_SCRUB ? "EZFS_NO_SCRUB" :
	r == EZFS_DIFF ? "EZFS_DIFF" :
	r == EZFS_DIFFDATA ? "EZFS_DIFFDATA" :
	r == EZFS_POOLREADONLY ? "EZFS_POOLREADONLY" :
	r == EZFS_SCRUB_PAUSED ? "EZFS_SCRUB_PAUSED" :
	r == EZFS_ACTIVE_POOL ? "EZFS_ACTIVE_POOL" :
	r == EZFS_CRYPTOFAILED ? "EZFS_CRYPTOFAILED" :
	r == EZFS_NO_PENDING ? "EZFS_NO_PENDING" :
	r == EZFS_CHECKPOINT_EXISTS ? "EZFS_CHECKPOINT_EXISTS" :
	r == EZFS_DISCARDING_CHECKPOINT ? "EZFS_DISCARDING_CHECKPOINT" :
	r == EZFS_NO_CHECKPOINT ? "EZFS_NO_CHECKPOINT" :
	r == EZFS_DEVRM_IN_PROGRESS ? "EZFS_DEVRM_IN_PROGRESS" :
	r == EZFS_VDEV_TOO_BIG ? "EZFS_VDEV_TOO_BIG" :
	r == EZFS_TOOMANY ? "EZFS_TOOMANY" :
	r == EZFS_INITIALIZING ? "EZFS_INITIALIZING" :
	r == EZFS_NO_INITIALIZE ? "EZFS_NO_INITIALIZE" :
	r == EZFS_WRONG_PARENT ? "EZFS_WRONG_PARENT" :
	r == EZFS_TRIMMING ? "EZFS_TRIMMING" :
	r == EZFS_NO_TRIM ? "EZFS_NO_TRIM" :
	r == EZFS_TRIM_NOTSUP ? "EZFS_TRIM_NOTSUP" :
	r == EZFS_NO_RESILVER_DEFER ? "EZFS_NO_RESILVER_DEFER" :
	r == EZFS_IOC_NOTSUPPORTED ? "EZFS_IOC_NOTSUPPORTED" :
	"EZFS_UNKNOWN";

pid$target::zfs_error:entry,
pid$target::zfs_verror:entry
{
	printf("pid %d: error %d %s\n", pid, arg1, enam[arg1]);
	ustack();
	printf("\n");

	if (arg1 == EZFS_NOSPC) {
		system("echo ## ZFS LIST; zfs list; echo");
		system("echo ## ZPOOL LIST; zpool list; echo");
		system("echo ## DF; df -h; echo");
		system("echo ## SWAP; swap -sh; swap -lh; echo");
		system("echo ## MDB MEMSTAT; mdb -ke ::memstat; echo");
		system("echo ##; echo");
	}
}
