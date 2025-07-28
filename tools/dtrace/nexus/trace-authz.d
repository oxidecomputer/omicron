#!/usr/sbin/dtrace -qZs

/*
 * This script prints the result of every authorization request. It
 * includes the actor, action, and resource, along with the duration
 * and result of the authz attempt. Here is an example of the output:
 *
 * actor=Some(Actor::SiloUser { silo_user_id: 8b0bc366-958e-43af-a35c-8cb6867a6939, silo_id: 37ab44ad-213c-482d-acf4-98e6a978199d, .. }) action=Query resource=Database result=Ok(()) duration=1351us
 * actor=Some(Actor::SiloUser { silo_user_id: 001de000-05e4-4000-8000-000000004007, silo_id: 001de000-5110-4000-8000-000000000000, .. }) action=Query resource=Database result=Ok(()) duration=657us
 * actor=Some(Actor::SiloUser { silo_user_id: 001de000-05e4-4000-8000-000000004007, silo_id: 001de000-5110-4000-8000-000000000000, .. }) action=Query resource=Database result=Ok(()) duration=1640us
 * actor=Some(Actor::UserBuiltin { user_builtin_id: 001de000-05e4-4000-8000-000000000002, .. }) action=Read resource=DnsConfig result=Ok(()) duration=12268us
 *
 */

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
