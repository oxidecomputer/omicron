#!/usr/sbin/dtrace -Zqs

nexus*:::background-task-activate-start
{
        this->task_name = copyinstr(arg0);
        iters[this->task_name] = arg1;
        reasons[this->task_name] = copyinstr(arg2);
        ts[this->task_name] = timestamp;
}

nexus*:::background-task-activate-done
/iters[this->task_name]/
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
