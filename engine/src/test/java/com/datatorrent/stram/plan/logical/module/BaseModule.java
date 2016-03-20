package com.datatorrent.stram.plan.logical.module;

import com.datatorrent.api.Context;
import com.datatorrent.api.Module;

abstract class BaseModule implements Module {
    @Override
    public void beginWindow(long windowId) {

    }

    @Override
    public void endWindow() {

    }

    @Override
    public void setup(Context.OperatorContext context) {

    }

    @Override
    public void teardown() {

    }
}
