
const { Ryoiki } = require('ryoiki');

async function test() {
  const ryoiki = new Ryoiki();
  let firstLockId = '';

  console.log('1. Acquire Read Lock [0, 1]');
  // Callback returns IMMEDIATELY
  await ryoiki.readLock([0, 1], async (lockId) => {
    console.log('   -> Read Lock acquired:', lockId);
    firstLockId = lockId;
  });

  console.log('2. Callback returned. Checking if lock is still held...');

  let secondLockAcquired = false;
  const writeLockPromise = ryoiki.writeLock([0, 1], async (lockId) => {
    console.log('   -> Write Lock acquired:', lockId);
    secondLockAcquired = true;
  });

  // Wait 1 second
  await new Promise(r => setTimeout(r, 1000));

  if (secondLockAcquired) {
    console.log('RESULT: Lock was AUTO-RELEASED. (Behavior: Auto-Release)');
  } else {
    console.log('RESULT: Lock PERSISTS. (Behavior: Manual Release Required)');
    // Cleanup
    ryoiki.readUnlock(firstLockId);
    await writeLockPromise;
  }
}

test();
