/*
 * Copyright (c) 2026 by C. Klukas.
 * Licensed under the MIT License. See LICENSE for details.
 */

(function () {
  "use strict";

  const NAV_CLASS = "paglets-section-pages";

  function activeSectionItem() {
    return document.querySelector(
      ".md-sidebar--primary nav.md-nav--primary > .md-nav__list > .md-nav__item--active"
    );
  }

  function sectionPageLinks(section) {
    const list = section.querySelector(":scope > nav.md-nav > .md-nav__list");
    if (!list) {
      return [];
    }
    return Array.from(list.children)
      .map((item) => {
        const link = item.querySelector(":scope > a.md-nav__link");
        if (!link) {
          return null;
        }
        return {
          active: item.classList.contains("md-nav__item--active"),
          href: link.getAttribute("href") || link.href,
          text: link.textContent.trim(),
        };
      })
      .filter((item) => item && item.text && item.href);
  }

  function renderSectionNav() {
    document.querySelectorAll(`.${NAV_CLASS}`).forEach((node) => node.remove());

    const section = activeSectionItem();
    const content = document.querySelector(".md-content");
    const article = content ? content.querySelector(".md-content__inner") : null;
    if (!section || !content || !article) {
      return;
    }

    const links = sectionPageLinks(section);
    if (links.length <= 1) {
      return;
    }

    const nav = document.createElement("nav");
    nav.className = NAV_CLASS;
    nav.setAttribute("aria-label", "Section pages");

    const list = document.createElement("ul");
    list.className = `${NAV_CLASS}__list`;
    links.forEach((item) => {
      const entry = document.createElement("li");
      const link = document.createElement("a");
      link.className = `${NAV_CLASS}__link`;
      if (item.active) {
        link.classList.add(`${NAV_CLASS}__link--active`);
      }
      link.href = item.href;
      link.textContent = item.text;
      entry.appendChild(link);
      list.appendChild(entry);
    });
    nav.appendChild(list);

    content.insertBefore(nav, article);
  }

  function scheduleRender() {
    window.requestAnimationFrame(renderSectionNav);
  }

  if (typeof document$ !== "undefined" && document$.subscribe) {
    document$.subscribe(scheduleRender);
  } else if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", scheduleRender);
  } else {
    scheduleRender();
  }
})();
