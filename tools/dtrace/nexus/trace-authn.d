#!/usr/sbin/dtrace -qZs

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
