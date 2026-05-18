---
id: intro
slug: /
title: RETL docs overview
sidebar_label: Overview
hide_title: true
description: Task-oriented documentation for installing RETL skills and using an AI agent to build and operate syncs.
---

<div className="heroDoc">
  <div className="heroDoc__inner">
    <div className="heroDoc__copy">
      <div className="heroDoc__eyebrow">Local user documentation</div>
      <h1>Install RETL skills, then let an AI agent build the sync with you.</h1>
      <div className="heroDoc__summary">
        RETL ships project-local AI skills for setup, sync authoring,
        destination wiring, debugging, and runtime operations. Start by
        installing those skills into your repository, then ask your agent to use
        them against your actual warehouse, destination, and operating model.
      </div>
      <div className="heroDoc__actions">
        <a className="heroDoc__button heroDoc__button--primary" href="/quickstart/">Install skills</a>
        <a className="heroDoc__button heroDoc__button--secondary" href="/guides/first-sync/">First sync guide</a>
      </div>
    </div>
    <div className="heroDoc__panel" aria-label="Documentation path">
      <div className="heroDoc__panelTitle">Documentation path</div>
      <ol className="heroDoc__steps">
        <li>
          <span className="heroDoc__stepIndex">01</span>
          <span className="heroDoc__stepText">Install RETL and run <code>retl install-skills</code>.</span>
        </li>
        <li>
          <span className="heroDoc__stepIndex">02</span>
          <span className="heroDoc__stepText">Ask your agent to use <code>retl-start-project</code>.</span>
        </li>
        <li>
          <span className="heroDoc__stepIndex">03</span>
          <span className="heroDoc__stepText">Review the dry-run plan before writing to a destination.</span>
        </li>
      </ol>
    </div>
  </div>
</div>

## Start here

- [Install skills](./quickstart.md) is the primary path for using RETL with an
  AI agent in your own project.
- [Install RETL](./guides/install.md) covers the package, backend extras,
  connector packages, and skill install command.
- [Create a first sync](./guides/first-sync.md) shows what the agent is
  expected to produce: source, State or Event intent, destination binding, Sync,
  and dry-run runner code.
- [Dry runs and recovery](./guides/dry-runs-and-recovery.md) explains how to
  inspect plans before writing to a destination and how retry behavior works.

## What this site owns

This site owns public user documentation: task flows, examples, concepts, and
reference tables needed to use RETL. Repository policy, implementation
contracts, and contributor rules live in the root `docs/` tree.
