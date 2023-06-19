package com.github.benmanes.caffeine.cache.simulator.admission;

import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;

public class UneditableAdmittorProxy implements Admittor {
    final private Admittor admittor;
    public UneditableAdmittorProxy(Admittor other) {
        admittor = other;
    }

    @Override
    public void record(AccessEvent event) {
        // Do nothing
    }

    @Override
    public void record(long key) {
        // Do nothing
    }

    @Override
    public boolean admit(AccessEvent candidate, AccessEvent victim) {
        return admittor.admit(candidate, victim);
    }

    @Override
    public boolean admit(long candidateKey, long victimKey) {
        return admittor.admit(candidateKey, victimKey);
    }
}
