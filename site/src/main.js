import "@primer/css/dist/base.css";
import "@primer/css/dist/buttons.css";
import "./styles.css";

const queryExamples = {
  graphql: `query FindPerson($request: QueryRequest!) {
  graph(request: $request) {
    version
    results
    stats
  }
}`,
  json: `{
  "op": "neighbors",
  "id": "person:alice",
  "direction": "out",
  "relation_types": ["works_at"],
  "project": ["id", "name"],
  "limit": 10
}`,
  legacy: `TRAVERSE service:checkout OUT
REL depends_on
DEPTH 3
PATH NODES service, host, database
END KIND database
LIMIT 50`,
};

const themeButton = document.querySelector("[data-theme-toggle]");
const themeMedia = window.matchMedia("(prefers-color-scheme: dark)");

function currentTheme() {
  return (
    document.documentElement.dataset.theme ||
    (themeMedia.matches ? "dark" : "light")
  );
}

function updateThemeButton() {
  if (!themeButton) return;
  const nextTheme = currentTheme() === "dark" ? "light" : "dark";
  themeButton.textContent =
    nextTheme === "dark" ? "Dark mode" : "Light mode";
  themeButton.setAttribute(
    "aria-label",
    `Switch to ${nextTheme} color theme`,
  );
}

themeButton?.addEventListener("click", () => {
  const nextTheme = currentTheme() === "dark" ? "light" : "dark";
  document.documentElement.dataset.theme = nextTheme;
  try {
    localStorage.setItem("ggraphdb-theme", nextTheme);
  } catch {}
  updateThemeButton();
});

themeMedia.addEventListener("change", () => {
  if (!document.documentElement.dataset.theme) {
    updateThemeButton();
  }
});

updateThemeButton();

const navToggle = document.querySelector(".nav-toggle");
const primaryNav = document.querySelector(".primary-nav");

function closeNavigation() {
  navToggle?.setAttribute("aria-expanded", "false");
  if (navToggle) navToggle.textContent = "Menu";
  primaryNav?.removeAttribute("data-open");
}

navToggle?.addEventListener("click", () => {
  const expanded = navToggle.getAttribute("aria-expanded") === "true";
  navToggle.setAttribute("aria-expanded", String(!expanded));
  navToggle.textContent = expanded ? "Menu" : "Close";
  primaryNav?.toggleAttribute("data-open", !expanded);
});

primaryNav?.querySelectorAll("a").forEach((link) => {
  link.addEventListener("click", closeNavigation);
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") closeNavigation();
});

const codeOutput = document.querySelector("[data-code-output]");
const codeTabs = document.querySelectorAll("[data-code-tab]");

function selectCodeExample(name) {
  if (!codeOutput || !queryExamples[name]) return;
  codeOutput.textContent = queryExamples[name];
  codeTabs.forEach((tab) => {
    tab.setAttribute(
      "aria-selected",
      String(tab.dataset.codeTab === name),
    );
  });
}

codeTabs.forEach((tab) => {
  tab.addEventListener("click", () => {
    selectCodeExample(tab.dataset.codeTab);
    document.querySelector("[data-copy-status]").textContent = "";
  });
});

selectCodeExample("graphql");

async function copyText(text, button, statusNode) {
  const originalLabel = button.textContent;
  try {
    await navigator.clipboard.writeText(text);
    button.textContent = "Copied";
    statusNode.textContent = "Copied to clipboard.";
  } catch {
    button.textContent = "Copy failed";
    statusNode.textContent = "Clipboard access was unavailable. Select the text manually.";
  }

  window.setTimeout(() => {
    button.textContent = originalLabel;
    statusNode.textContent = "";
  }, 1800);
}

const copyCodeButton = document.querySelector("[data-copy-code]");
const copyCodeStatus = document.querySelector("[data-copy-status]");

copyCodeButton?.addEventListener("click", () => {
  copyText(codeOutput?.textContent || "", copyCodeButton, copyCodeStatus);
});

const copyCommandButton = document.querySelector("[data-copy-command]");
const commandOutput = document.querySelector("[data-command]");
const commandStatus = document.querySelector("[data-command-status]");

copyCommandButton?.addEventListener("click", () => {
  copyText(
    commandOutput?.textContent || "",
    copyCommandButton,
    commandStatus,
  );
});

const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");
const revealItems = document.querySelectorAll(".reveal");

if (reducedMotion.matches || !("IntersectionObserver" in window)) {
  revealItems.forEach((item) => item.classList.add("is-visible"));
} else {
  const revealObserver = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        entry.target.classList.add("is-visible");
        revealObserver.unobserve(entry.target);
      });
    },
    { threshold: 0.16 },
  );

  revealItems.forEach((item) => revealObserver.observe(item));
}

document.querySelector("[data-year]").textContent =
  new Date().getFullYear();
