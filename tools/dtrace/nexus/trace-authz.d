#!/usr/sbin/dtrace -qZs

#pragma D option strsize=4k

nexus*:::authz-start
{
        ts[arg0] = timestamp;
        actors[arg0] = copyinstr(arg1);
        actions[arg0] = copyinstr(arg2);
        resources[arg0] = copyinstr(arg3);
}

nexus*:::authz-done
/ts[arg0]/
{
        t = (timestamp - ts[arg0]);
        printf(
            "actor=%s action=%s resource=%s result=%s duration=%dus\n",
            actors[arg0],
            actions[arg0],
            resources[arg0],
            copyinstr(arg1),
            t / 1000
        );
        ts[arg0] = 0;
        actors[arg0] = 0;
        actions[arg0] = 0;
        resources[arg0] = 0;
}
