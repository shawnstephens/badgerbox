# Full-filesystem admission test

The opt-in `resourcefault` test fills a marked disposable filesystem until an
actual write returns `ENOSPC`. It verifies that the free-space guard rejects
`Enqueue` and `EnqueueTx`, application writes in the rejected transaction roll
back, quota counters stay unchanged, and all previously accepted messages
survive reopening and then deliver exactly once in this test. Final retained
usage is zero and the audit independently reconciles it.

The fixture must be a separate mount between 128 MiB and 1 GiB, with the marker
below. The test rejects the workspace filesystem and only writes inside its
own newly created directory. Filler is removed before database cleanup even
when an assertion fails. Use a disposable image or tmpfs reserved for this test.

On macOS, from the repository root:

```sh
fixture=$(mktemp -d /tmp/badgerbox-disk-fault.XXXXXX)
mkdir "$fixture/mount"
hdiutil create -size 512m -fs APFS -type SPARSE -volname BadgerboxFault "$fixture/fixture.sparseimage"
hdiutil attach -nobrowse -mountpoint "$fixture/mount" "$fixture/fixture.sparseimage"
printf 'badgerbox resource-fault fixture v1\n' > "$fixture/mount/.badgerbox-disposable-volume"
BADGERBOX_TEST_VOLUME="$fixture/mount" GOWORK=off go test -race -v \
  -tags=resourcefault ./tests/integration \
  -run '^TestFullFilesystemRejectsIntakeAndRecoversRetainedMessages$' \
  -count=1 -timeout=2m
hdiutil detach "$fixture/mount"
```

Preserve the test output and image if investigation is needed. After detaching,
the directory contains only this disposable fixture and can be removed. If the
test is interrupted externally, detach its mount explicitly. Newer macOS
versions may emit deprecation messages for `hdiutil`; use the corresponding
`diskutil image` commands on deployments that remove it. On Linux, provision a
dedicated size-limited tmpfs mount, add the same marker, and use the same Go test
command. Mount provisioning requires the host's appropriate permissions.

This check proves the configured admission policy at filesystem exhaustion and
recovery after space is restored. It does not inject a device failure, power
loss, or a synchronization error after admission succeeds. Available space can
change between a probe and commit; handle every Badger write/commit error and
reserve headroom for settlement and GC. A volume returning `ENOSPC` can still
report reserved free space unavailable to an ordinary write.
