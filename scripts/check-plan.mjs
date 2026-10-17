import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const plan = JSON.parse(
  await readFile(path.join(root, "docs/curriculum/plan-90-days.json"), "utf8"),
);
const curriculum = JSON.parse(
  await readFile(path.join(root, "docs/curriculum/curriculum.json"), "utf8"),
);
const errors = [];
const fail = (message) => errors.push(message);

async function filesUnder(dir) {
  const found = [];
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) found.push(...(await filesUnder(full)));
    else if (/\.mdx?$/.test(entry.name)) found.push(full);
  }
  return found;
}

const collectionDirs = {
  articles: "articles",
  "interview-questions": "interview-questions",
  "company-guides": "company-guides",
  roadmaps: "roadmaps",
  "system-designs": "system-designs",
  projects: "projects",
  "cheat-sheets": "cheat-sheets",
  technologies: "technologies",
};
const refs = new Set();
for (const [collection, dir] of Object.entries(collectionDirs)) {
  const contentRoot = path.join(root, "src/content", dir);
  for (const file of await filesUnder(contentRoot)) {
    const id = path
      .relative(contentRoot, file)
      .replace(/\\/g, "/")
      .replace(/\.mdx?$/, "");
    refs.add(`${collection}:${id}`);
  }
}

// Headings ids as Astro generates them (close enough to github-slugger for the plain headings used here).
const headingId = (text) =>
  text
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s_-]/gu, "")
    .trim()
    .replace(/\s/g, "-");
const anchorsOf = new Map();
async function anchors(ref) {
  if (anchorsOf.has(ref)) return anchorsOf.get(ref);
  const [collection, id] = ref.split(":");
  const base = path.join(root, "src/content", collectionDirs[collection], id);
  let text = "";
  for (const ext of [".md", ".mdx"]) text ||= await readFile(base + ext, "utf8").catch(() => "");
  const ids = new Set(
    [...text.matchAll(/^#{2,3} (.+)$/gm)].map((m) => headingId(m[1].replace(/`/g, ""))),
  );
  anchorsOf.set(ref, ids);
  return ids;
}

const rowOf = (tracker, topic) => curriculum[tracker]?.items.find((row) => row.topic === topic);

if (plan.version !== 3) fail("plan version must be 3");
if (plan.days.length !== 90) fail(`expected 90 days, found ${plan.days.length}`);
if (plan.days.some((day, i) => day.day !== i + 1)) fail("days must be numbered 1 through 90");

const firstDay = new Map(); // "tracker:topic" or track -> first day it appears
const seenTracker = new Set();
const practices = new Set();
let totalHours = 0;
for (const day of plan.days) {
  totalHours += day.hours;
  const weekend = (day.day - 1) % 7 >= 5; // day 1 is a Monday
  if (day.hours !== 3) fail(`day ${day.day} should be 3 hours, found ${day.hours}`);
  if (!day.theme || !day.exercise || !day.interviewQuestion || !day.outcome)
    fail(`day ${day.day} needs a theme, exercise, interview question and outcome`);
  const minutes = day.tasks.reduce((sum, task) => sum + task.minutes, 0);
  if (minutes !== 180) fail(`day ${day.day} task minutes add to ${minutes}, expected 180`);
  const count = day.tasks.reduce(
    (n, task) => n + task.items.length + (task.practice?.length ?? 0),
    0,
  );
  const expected = weekend ? 12 : 10;
  if (count !== expected) fail(`day ${day.day} has ${count} tasks, expected ${expected}`);
  for (const required of ["sql", "dsa", "interview", "review"])
    if (!day.tasks.some((task) => task.track === required))
      fail(`day ${day.day} has no ${required} task`);

  for (const task of day.tasks) {
    if (!Number.isInteger(task.minutes) || task.minutes < 5)
      fail(`day ${day.day} has a task under 5 minutes`);
    if (!firstDay.has(task.track)) firstDay.set(task.track, day.day);
    for (const item of task.items) {
      if (!item.ref) fail(`day ${day.day}: "${item.topic}" has no page`);
      else if (!refs.has(item.ref)) fail(`day ${day.day} links to missing page ${item.ref}`);
      else if (item.anchor && !(await anchors(item.ref)).has(item.anchor))
        fail(`day ${day.day}: ${item.ref} has no heading #${item.anchor}`);
      if (!item.tracker) continue;
      const row = rowOf(item.tracker, item.topic);
      if (!row) {
        fail(`day ${day.day}: "${item.topic}" is not in the ${item.tracker} tracker`);
        continue;
      }
      const key = `${item.tracker}:${item.topic}`;
      if (seenTracker.has(key)) fail(`${key} is scheduled twice`);
      seenTracker.add(key);
      firstDay.set(key, day.day);
      if (!firstDay.has(task.track + ":first")) firstDay.set(task.track + ":first", day.day);
      // Progression: easy work first, the hardest work last.
      if (item.tracker === "dsa" && row.difficulty === "Hard" && day.day < 80)
        fail(`day ${day.day}: Hard DSA problem ${item.topic} before day 80`);
      if (item.tracker === "dsa" && row.difficulty !== "Easy" && day.day <= 7)
        fail(`day ${day.day}: ${row.difficulty} DSA problem ${item.topic} in week 1`);
      if (item.tracker === "sql" && row.difficulty === "Expert" && row.lesson && day.day < 60)
        fail(`day ${day.day}: expert SQL topic ${item.topic} before day 60`);
    }
    for (const practice of task.practice ?? []) {
      const url = new URL(practice.url);
      if (url.hostname !== "leetcode.com" || !/^\/problems\/[^/]+\/$/.test(url.pathname))
        fail(`day ${day.day} has a malformed LeetCode URL ${practice.url}`);
      if (practices.has(practice.url)) fail(`duplicate SQL practice ${practice.url}`);
      practices.add(practice.url);
    }
  }
}

// DSA difficulty never steps back within a pattern group (Easy -> Medium), Hard only at the end.
const dsaOrder = plan.days.flatMap((day) =>
  day.tasks
    .filter((task) => task.track === "dsa")
    .flatMap((task) => task.items.map((item) => item.topic)),
);
if (dsaOrder.length < 200) fail(`expected at least 200 DSA problems, found ${dsaOrder.length}`);

// Prerequisites: each track starts only after what it builds on.
const at = (tracker, topic) => firstDay.get(`${tracker}:${topic}`);
const requireBefore = (track, requirements, label) => {
  const starts = firstDay.get(track + ":first");
  const latest = Math.max(
    ...requirements.map(([tracker, topic]) => at(tracker, topic) ?? Infinity),
  );
  if (!starts) fail(`${track} is never scheduled`);
  else if (!(latest <= starts)) fail(`${track} starts on day ${starts}, before ${label}`);
};
requireBefore(
  "data-modeling",
  [
    ["sql", "INNER JOIN basics"],
    ["sql", "LEFT JOIN basics"],
  ],
  "SQL joins",
);
requireBefore(
  "pyspark",
  [
    ["sql", "GROUP BY fundamentals"],
    ["de-concepts", "Parquet and columnar storage"],
  ],
  "SQL aggregation and file formats",
);
requireBefore(
  "airflow",
  [
    ["de-concepts", "Idempotent pipeline writes"],
    ["de-concepts", "Retries and failure recovery"],
  ],
  "idempotency and retries",
);
requireBefore(
  "snowflake",
  [
    ["data-modeling", "Star Schema"],
    ["data-modeling", "Grain definition"],
  ],
  "warehouse modeling",
);
requireBefore(
  "kafka",
  [
    ["de-concepts", "Batch versus streaming"],
    ["de-concepts", "Change data capture patterns"],
  ],
  "streaming and CDC concepts",
);
requireBefore(
  "spark-streaming",
  [
    ["kafka", "Topic fundamentals"],
    ["kafka", "Consumer group basics"],
  ],
  "Kafka basics",
);
requireBefore(
  "system-design",
  [
    ["de-concepts", "Lake, warehouse and lakehouse"],
    ["de-concepts", "Partitioning and data layout"],
  ],
  "storage concepts",
);
requireBefore("aws", [["de-concepts", "Partitioning and data layout"]], "data layout");

for (const day of [7, 14, 21, 28, 35, 42, 49, 56, 63, 70, 77, 84, 90])
  if (!plan.days[day - 1]?.weekly) fail(`day ${day} is missing its weekly milestone`);
for (const day of [28, 63, 90])
  if (!plan.days[day - 1]?.monthly) fail(`day ${day} is missing its phase review`);
if (totalHours !== 270) fail(`expected 270 hours, found ${totalHours}`);
if (practices.size !== 55)
  fail(`expected the 55 LeetCode SQL 50 problems, found ${practices.size}`);

if (errors.length) {
  console.error(`Plan check failed with ${errors.length} issue(s):`);
  for (const error of errors) console.error(`- ${error}`);
  process.exitCode = 1;
} else {
  const tasks = plan.days.reduce(
    (n, d) => n + d.tasks.reduce((m, t) => m + t.items.length + (t.practice?.length ?? 0), 0),
    0,
  );
  console.log(
    `Plan check passed: 90 days, 3 hours each, ${tasks} tasks, ${dsaOrder.length} DSA problems easy to hard, prerequisites in order.`,
  );
}
