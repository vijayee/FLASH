/**
 * LRU-ish pool of established probe connections (spec §2.5, §3.3).
 * Entries: { targetId, pc, dc, lastUsed, createdAt }.
 */
export class ConnectionPool {
  constructor(maxSize = 10) {
    this.entries = [];
    this.maxSize = maxSize;
  }

  find(targetId) {
    return this.entries.find((entry) => entry.targetId === targetId) || null;
  }

  add(entry) {
    if (this.entries.length >= this.maxSize) {
      let oldestIndex = 0;
      for (let i = 1; i < this.entries.length; i++) {
        if (this.entries[i].lastUsed < this.entries[oldestIndex].lastUsed) {
          oldestIndex = i;
        }
      }
      const [oldest] = this.entries.splice(oldestIndex, 1);
      try {
        oldest.pc.close();
      } catch {
        // Already closed.
      }
    }
    this.entries.push(entry);
  }

  cleanup(maxAgeMs = 120000) {
    const now = Date.now();
    this.entries = this.entries.filter((entry) => {
      if (now - entry.lastUsed > maxAgeMs) {
        try {
          entry.pc.close();
        } catch {
          // Already closed.
        }
        return false;
      }
      return true;
    });
  }

  close() {
    for (const entry of this.entries) {
      try {
        entry.pc.close();
      } catch {
        // Already closed.
      }
    }
    this.entries = [];
  }
}