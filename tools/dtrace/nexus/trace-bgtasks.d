#!/usr/sbin/dtrace -Zqs

/* This script prints the result of every background task's activation.
 * It includes the task name, its iteration, the reason the task ran,
 * and the result and duration of the execution. Here's an example:
 *
 * task_name="dns_servers_internal" iteration=2 reason=Signaled details={"addresses":["[::1]:56144"]} duration=22054us
 * task_name="dns_servers_external" iteration=2 reason=Signaled details={"addresses":["[::1]:56161"]} duration=21254us
 * task_name="dns_propagation_internal" iteration=2 reason=Dependency details={"error":"no config"} duration=511us
 * task_name="dns_propagation_external" iteration=2 reason=Dependency details={"error":"no config"} duration=19us
 * task_name="v2p_manager" iteration=13 reason=Signaled details={} duration=432252us
 * task_name="dns_servers_internal" iteration=2 reason=Signaled details={"addresses":["[::1]:36483"]} duration=21347us
 * task_name="dns_servers_external" iteration=2 reason=Signaled details={"addresses":["[::1]:50606"]} duration=20789us
 * task_name="dns_propagation_internal" iteration=2 reason=Dependency details={"error":"no config"} duration=38us
 * task_name="dns_propagation_external" iteration=2 reason=Dependency details={"error":"no config"} duration=11us
 * task_name="vpc_route_manager" iteration=10 reason=Signaled details={} duration=485381us
 */

nexus*:::background-task-activate-start
{
        this->task_name = copyinstr(arg0);
        iters[this->task_name] = arg1;
        reasons[this->task_name] = copyinstr(arg2);
        ts[this->task_name] = timestamp;
}

nexus*:::background-task-activate-done
/iters[copyinstr(arg0)] == arg1/
{
        this->task_name = copyinstr(arg0);
        this->duration = timestamp - ts[this->task_name];
        printf(
            "task_name=\"%s\" iteration=%d reason=%s details=%s duration=%dus\n",
            this->task_name,
            iters[this->task_name],
            reasons[this->task_name],
            copyinstr(arg2),
            this->duration / 1000
        );
        iters[this->task_name] = 0;
        reasons[this->task_name] = 0;
        ts[this->task_name] = 0;
}
