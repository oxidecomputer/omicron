#!/usr/sbin/dtrace -qZs

/*
 * This script prints the result of every authorization request. It
 * includes the actor, action, and resource, along with the duration
 * and result of the authz attempt. Here is an example of the output:
 *
 * request_id=none actor=Some(Actor::UserBuiltin { user_builtin_id: 001de000-05e4-4000-8000-000000000003, .. }) action=Query resource=Database result=Ok(()) duration=980us
 * request_id=none actor=Some(Actor::UserBuiltin { user_builtin_id: 001de000-05e4-4000-8000-000000000002, .. }) action=Query resource=Database result=Ok(()) duration=1067us
 * request_id=464df995-d044-4c23-a474-3ad3272a0de1 actor=Some(Actor::SiloUser { silo_user_id: 001de000-05e4-4000-8000-000000004007, silo_id: 001de000-5110-4000-8000-000000000000, .. }) action=Query resource=Database result=Ok(()) duration=881us
 * request_id=none actor=Some(Actor::UserBuiltin { user_builtin_id: 001de000-05e4-4000-8000-000000000002, .. }) action=Query resource=Database result=Ok(()) duration=841us
 *
 */

#pragma D option strsize=4k

nexus*:::authz-start
{
        ts[arg0] = timestamp;
        rqids[arg0] = copyinstr(arg1);
        actors[arg0] = copyinstr(arg2);
        actions[arg0] = copyinstr(arg3);
        resources[arg0] = copyinstr(arg4);
}

nexus*:::authz-done
/ts[arg0]/
{
        t = (timestamp - ts[arg0]);
        printf(
            "request_id=%s actor=%s action=%s resource=%s result=%s duration=%dus\n",
            rqids[arg0],
            actors[arg0],
            actions[arg0],
            resources[arg0],
            copyinstr(arg1),
            t / 1000
        );
        ts[arg0] = 0;
        rqids[arg0] = 0;
        actors[arg0] = 0;
        actions[arg0] = 0;
        resources[arg0] = 0;
}
