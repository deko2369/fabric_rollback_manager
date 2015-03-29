# -*- coding: utf-8 -*-
from fabric.api import task

from rollback_manager import RollbackManager

@task
def rollback():
    rollback_manager = RollbackManager('/path/to/file')
    rollback_manager.rollback()

@task
def commit():
    rollback_manager = RollbackManager('/path/to/file')
    rollback_manager.commit()

