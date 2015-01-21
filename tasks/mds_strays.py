import contextlib
import logging
from textwrap import dedent
import time
from tasks.cephfs.cephfs_test_case import run_tests, CephFSTestCase
from tasks.cephfs.filesystem import Filesystem

log = logging.getLogger(__name__)


class TestStrays(CephFSTestCase):
    OPS_THROTTLE = 1
    FILES_THROTTLE = 2

    def test_ops_throttle(self):
        self._test_throttling(self.OPS_THROTTLE)

    def test_files_throttle(self):
        self._test_throttling(self.FILES_THROTTLE)

    def _test_throttling(self, throttle_type):
        """
        That the mds_max_purge_ops setting is respected
        """

        # Lift the threshold on whatever throttle we are *not* testing, so
        # that the throttle of interest is the one that will be the bottleneck
        if throttle_type == self.OPS_THROTTLE:
            self.set_conf('mds', 'mds_max_purge_files', "100000000")
        elif throttle_type == self.FILES_THROTTLE:
            self.set_conf('mds', 'mds_max_purge_ops', "100000000")
        else:
            raise NotImplemented(throttle_type)

        create_script = dedent("""
            import os

            mount_path = "{mount_path}"
            subdir = "delete_me"
            MB = 1024 * 1024
            os.mkdir(os.path.join(mount_path, subdir))
            for i in range(0, 25):
                for size in range(0, 16*MB, MB):
                    filename = "{{0}}_{{1}}MB.bin".format(i, size / MB)
                    f = open(os.path.join(mount_path, subdir, filename), 'w')
                    f.write(size * 'x')
                    f.close()
        """.format(
            mount_path=self.mount_a.mountpoint
        ))

        self.mount_a.run_python(create_script)
        self.mount_a.run_shell(["rm", "-rf", "delete_me"])
        self.fs.mds_asok(["flush", "journal"])

        total_inodes = 401
        mds_max_purge_ops = int(self.fs.get_config("mds_max_purge_ops"))
        mds_max_purge_files = int(self.fs.get_config("mds_max_purge_files"))

        # During this phase we look for the concurrent ops to exceed half
        # the limit (a heuristic) and not exceed the limit (a correctness
        # condition).
        purge_timeout = 600
        elapsed = 0
        files_high_water = 0
        ops_high_water = 0
        while True:
            mdc_stats = self.fs.mds_asok(['perf', 'dump'])['mds_cache']
            if elapsed >= purge_timeout:
                raise RuntimeError("Timeout waiting for {0} inodes to purge, stats:{1}".format(total_inodes, mdc_stats))

            num_strays = mdc_stats['num_strays']
            num_strays_purging = mdc_stats['num_strays_purging']
            num_purge_ops = mdc_stats['num_purge_ops']

            files_high_water = max(files_high_water, num_strays_purging)
            ops_high_water = max(ops_high_water, num_purge_ops)

            total_strays_created = mdc_stats['strays_created']
            total_strays_purged = mdc_stats['strays_purged']

            if total_strays_purged == total_inodes:
                log.info("Complete purge in {0} seconds".format(elapsed))
                break
            elif total_strays_purged > total_inodes:
                raise RuntimeError("Saw more strays than expected, mdc stats: {0}".format(mdc_stats))
            else:
                if throttle_type == self.OPS_THROTTLE:
                    if num_strays_purging > mds_max_purge_files:
                        raise RuntimeError("num_purge_ops violates threshold {0}/{1}".format(
                            num_purge_ops, mds_max_purge_ops
                        ))
                elif throttle_type == self.FILES_THROTTLE:
                    if num_strays_purging > mds_max_purge_files:
                        raise RuntimeError("num_strays_purging violates threshold {0}/{1}".format(
                            num_strays_purging, mds_max_purge_files
                        ))
                else:
                    raise NotImplemented(throttle_type)

                log.info("Waiting for purge to complete {0}/{1}, {2}/{3}".format(
                    num_strays_purging, num_strays,
                    total_strays_purged, total_strays_created
                ))
                time.sleep(1)
                elapsed += 1

        # Check that we got up to a respectable rate during the purge.  This is totally
        # racy, but should be safeish unless the cluster is pathalogically slow, or
        # insanely fast such that the deletions all pass before we have polled the
        # statistics.
        if throttle_type == self.OPS_THROTTLE:
            if ops_high_water < mds_max_purge_ops / 2:
                raise RuntimeError("Ops in flight high water is unexpectedly low ({0} / {1})".format(
                    ops_high_water, mds_max_purge_ops
                ))
        elif throttle_type == self.FILES_THROTTLE:
            if files_high_water < mds_max_purge_files / 2:
                raise RuntimeError("Files in flight high water is unexpectedly low ({0} / {1})".format(
                    ops_high_water, mds_max_purge_ops
                ))

        # Sanity check all MDC stray stats
        mdc_stats = self.fs.mds_asok(['perf', 'dump'])['mds_cache']
        self.assertEqual(mdc_stats['num_strays'], 0)
        self.assertEqual(mdc_stats['num_strays_purging'], 0)
        self.assertEqual(mdc_stats['num_strays_delayed'], 0)
        self.assertEqual(mdc_stats['num_purge_ops'], 0)
        self.assertEqual(mdc_stats['strays_created'], total_inodes)
        self.assertEqual(mdc_stats['strays_purged'], total_inodes)


@contextlib.contextmanager
def task(ctx, config):
    fs = Filesystem(ctx)

    # Pick out the clients we will use from the configuration
    # =======================================================
    if len(ctx.mounts) < 1:
        raise RuntimeError("Need at least one client")
    mount = ctx.mounts.values()[0]

    # Stash references on ctx so that we can easily debug in interactive mode
    # =======================================================================
    ctx.filesystem = fs
    ctx.mount = mount

    run_tests(ctx, config, TestStrays, {
        'fs': fs,
        'mount_a': mount,
    })

    # Continue to any downstream tasks
    # ================================
    yield
