#!/usr/sbin/dtrace -qZs

/*
 * This script prints the result of every authentication request. It
 * includes the scheme, method, URI, duration, and the result of the
 * authn attempt. Here is an example of the output:
 * 
 * scheme=spoof method=DELETE request_id=5fbe4092-1e91-4802-b4ae-0d1c969ed2b7 URI=/v1/disks/disky-mcdiskface?project=springfield-squidport result=Authenticated(Details { actor: Actor::SiloUser { silo_user_id: 001de000-05e4-4000-8000-000000004007, silo_id: 001de000-5110-4000-8000-000000000000, .. } }) duration=24811us
 * scheme=spoof method=POST request_id=641abbe8-5d16-4f4a-92cf-4b624be557d5 URI=/v1/disks?project=springfield-squidport result=Authenticated(Details { actor: Actor::SiloUser { silo_user_id: 001de000-05e4-4000-8000-000000004007, silo_id: 001de000-5110-4000-8000-000000000000, .. } }) duration=23130us
 * scheme=spoof method=DELETE request_id=cd21ecb1-0c6b-440d-a2bc-1842121db5bd URI=/v1/disks/disky-mcdiskface?project=springfield-squidport result=Authenticated(Details { actor: Actor::SiloUser { silo_user_id: 001de000-05e4-4000-8000-000000004007, silo_id: 001de000-5110-4000-8000-000000000000, .. } }) duration=20129us
 */

#pragma D option strsize=4k

nexus*:::authn-start
{
        this->rqid = copyinstr(arg0);
        ts[this->rqid] = timestamp;
        schemes[this->rqid] = copyinstr(arg1);
        methods[this->rqid] = copyinstr(arg2);
        uris[this->rqid] = copyinstr(arg3);
}

nexus*:::authn-done
/ts[copyinstr(arg0)]/
{
        this->rqid = copyinstr(arg0);
        this->t = (timestamp - ts[this->rqid]) / 1000;
        printf(
            "scheme=%s method=%s request_id=%s URI=%s result=%s duration=%dus\n",
            schemes[this->rqid],
            methods[this->rqid],
            this->rqid,
            uris[this->rqid],
            copyinstr(arg1),
            this->t
        );
        ts[this->rqid] = 0;
        schemes[this->rqid] = 0;
        methods[this->rqid] = 0;
        uris[this->rqid] = 0;
}
