#!/usr/sbin/dtrace -qZs

/*
 * This script prints the result of every authentication request. It
 * includes the scheme, method, URI, duration, and the result of the
 * authn attempt. Here is an example of the output:
 * 
 * scheme=spoof method=POST URI=/v1/system/ip-pools/default/silos result=Authenticated(Details { actor: Actor::SiloUser { silo_user_id: 001de000-05e4-4000-8000-000000004007, silo_id: 001de000-5110-4000-8000-000000000000, .. } }) duration=23961us
 * scheme=spoof method=POST URI=/v1/projects result=Authenticated(Details { actor: Actor::SiloUser { silo_user_id: 001de000-05e4-4000-8000-000000004007, silo_id: 001de000-5110-4000-8000-000000000000, .. } }) duration=31087us
 * scheme=spoof method=DELETE URI=/v1/disks/disky-mcdiskface?project=springfield-squidport result=Authenticated(Details { actor: Actor::SiloUser { silo_user_id: 001de000-05e4-4000-8000-000000004007, silo_id: 001de000-5110-4000-8000-000000000000, .. } }) duration=1033288us
 * scheme=spoof method=POST URI=/v1/instances?project=carcosa result=Authenticated(Details { actor: Actor::SiloUser { silo_user_id: 001de000-05e4-4000-8000-000000004007, silo_id: 001de000-5110-4000-8000-000000000000, .. } }) duration=14631us
 * scheme=spoof method=POST URI=/v1/disks?project=springfield-squidport result=Authenticated(Details { actor: Actor::SiloUser { silo_user_id: 001de000-05e4-4000-8000-000000004007, silo_id: 001de000-5110-4000-8000-000000000000, .. } }) duration=25946us
 */

#pragma D option strsize=4k

nexus*:::authn-start
{
        ts[arg0] = timestamp;
        schemes[arg0] = copyinstr(arg1);
        methods[arg0] = copyinstr(arg2);
        uris[arg0] = copyinstr(arg3);
}

nexus*:::authn-done
/ts[arg0]/
{
        this->t = (timestamp - ts[arg0]) / 1000;
        printf(
            "scheme=%s method=%s URI=%s result=%s duration=%dus\n",
            schemes[arg0],
            methods[arg0],
            uris[arg0],
            copyinstr(arg1),
            this->t
        );
        ts[arg0] = 0;
        schemes[arg0] = 0;
        methods[arg0] = 0;
        uris[arg0] = 0;
}
