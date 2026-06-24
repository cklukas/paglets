/*
 * Copyright (c) 2026 by C. Klukas.
 * Licensed under the MIT License. See LICENSE for details.
 */

(function () {
  "use strict";

  const FRAME_CLASS = "paglets-mermaid-frame";
  const EXPAND_CLASS = "paglets-diagram-expand";
  const OVERLAY_ID = "paglets-diagram-overlay";
  let expandedFrame = null;
  let expandedPlaceholder = null;
  let expandedAspectRatio = null;

  const expandIcon =
    '<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">' +
    '<path fill="currentColor" d="M5 5h6v2H8.41l3.3 3.29-1.42 1.42L7 8.41V11H5V5zm8 0h6v6h-2V8.41l-3.29 3.3-1.42-1.42 3.3-3.29H13V5zM7 15.59l3.29-3.3 1.42 1.42-3.3 3.29H11v2H5v-6h2v2.59zm10 0V13h2v6h-6v-2h2.59l-3.3-3.29 1.42-1.42 3.29 3.3z"></path>' +
    "</svg>";
  const closeIcon =
    '<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">' +
    '<path fill="currentColor" d="M18.3 5.71 12 12l6.3 6.29-1.41 1.41L10.59 13.41 4.29 19.7 2.88 18.29 9.17 12 2.88 5.71 4.29 4.3l6.3 6.29 6.3-6.29 1.41 1.41z"></path>' +
    "</svg>";

  function createOverlay() {
    const existing = document.getElementById(OVERLAY_ID);
    if (existing) {
      return existing;
    }

    const overlay = document.createElement("div");
    overlay.id = OVERLAY_ID;
    overlay.className = "paglets-diagram-overlay";
    overlay.setAttribute("aria-hidden", "true");
    overlay.setAttribute("role", "dialog");
    overlay.setAttribute("aria-label", "Expanded diagram");
    overlay.innerHTML =
      '<div class="paglets-diagram-overlay__panel">' +
      '<div class="paglets-diagram-overlay__bar">' +
      '<button type="button" class="paglets-diagram-close" aria-label="Close expanded diagram">' +
      closeIcon +
      "</button>" +
      "</div>" +
      '<div class="paglets-diagram-overlay__content"></div>' +
      "</div>";

    overlay.addEventListener("click", (event) => {
      if (event.target === overlay) {
        closeOverlay();
      }
    });
    overlay.querySelector(".paglets-diagram-close").addEventListener("click", closeOverlay);
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && overlay.getAttribute("aria-hidden") === "false") {
        closeOverlay();
      }
    });
    document.body.appendChild(overlay);
    return overlay;
  }

  function remPixels() {
    const fontSize = window.getComputedStyle(document.documentElement).fontSize;
    return Number.parseFloat(fontSize) || 16;
  }

  function frameAspectRatio(frame) {
    const rect = frame.getBoundingClientRect();
    const width = Math.max(rect.width, 1);
    const height = Math.max(rect.height, 1);
    return width / height;
  }

  function resizeOverlayPanel(overlay, aspectRatio) {
    const panel = overlay.querySelector(".paglets-diagram-overlay__panel");
    if (!panel || !aspectRatio) {
      return;
    }

    const rem = remPixels();
    const overlayPadding = 4 * rem;
    const panelMaxWidth = Math.max(18 * rem, Math.min(window.innerWidth * 0.96, window.innerWidth - overlayPadding));
    const panelMaxHeight = Math.max(18 * rem, Math.min(window.innerHeight * 0.92, window.innerHeight - overlayPadding));
    const barHeight = 2.9 * rem;
    const contentPadding = 2 * rem;
    const contentMaxWidth = Math.max(12 * rem, panelMaxWidth - contentPadding);
    const contentMaxHeight = Math.max(12 * rem, panelMaxHeight - barHeight - contentPadding);

    let contentWidth = contentMaxWidth;
    let contentHeight = contentWidth / aspectRatio;
    if (contentHeight > contentMaxHeight) {
      contentHeight = contentMaxHeight;
      contentWidth = contentHeight * aspectRatio;
    }

    panel.style.width = `${Math.ceil(contentWidth + contentPadding)}px`;
    panel.style.height = `${Math.ceil(contentHeight + barHeight + contentPadding)}px`;
  }

  function resizeExpandedOverlay() {
    const overlay = document.getElementById(OVERLAY_ID);
    if (!overlay || overlay.getAttribute("aria-hidden") !== "false") {
      return;
    }
    resizeOverlayPanel(overlay, expandedAspectRatio);
  }

  function closeOverlay() {
    const overlay = document.getElementById(OVERLAY_ID);
    if (!overlay) {
      return;
    }
    if (expandedFrame && expandedPlaceholder) {
      expandedPlaceholder.parentNode.insertBefore(expandedFrame, expandedPlaceholder);
      expandedPlaceholder.remove();
    }
    expandedFrame = null;
    expandedPlaceholder = null;
    expandedAspectRatio = null;
    const panel = overlay.querySelector(".paglets-diagram-overlay__panel");
    if (panel) {
      panel.style.width = "";
      panel.style.height = "";
    }
    overlay.setAttribute("aria-hidden", "true");
    const content = overlay.querySelector(".paglets-diagram-overlay__content");
    if (content) {
      content.replaceChildren();
    }
    document.body.style.overflow = "";
  }

  function openOverlay(frame) {
    const aspectRatio = frameAspectRatio(frame);
    const overlay = createOverlay();
    const content = overlay.querySelector(".paglets-diagram-overlay__content");
    if (expandedFrame) {
      closeOverlay();
    }

    expandedPlaceholder = document.createComment("paglets expanded diagram placeholder");
    frame.parentNode.insertBefore(expandedPlaceholder, frame);
    expandedFrame = frame;
    expandedAspectRatio = aspectRatio;
    resizeOverlayPanel(overlay, aspectRatio);
    content.replaceChildren(frame);
    frame.querySelectorAll("svg").forEach((svg) => {
      if (svg.closest(`.${EXPAND_CLASS}`)) {
        return;
      }
      svg.removeAttribute("width");
      svg.removeAttribute("height");
      svg.setAttribute("preserveAspectRatio", "xMidYMid meet");
    });
    overlay.setAttribute("aria-hidden", "false");
    document.body.style.overflow = "hidden";
    const close = overlay.querySelector(".paglets-diagram-close");
    if (close) {
      close.focus();
    }
  }

  function diagramTargets(root) {
    return Array.from(root.querySelectorAll("pre.mermaid, div.mermaid")).filter(
      (node) =>
        !node.closest(`.${FRAME_CLASS}`) &&
        !node.closest(`#${OVERLAY_ID}`) &&
        node.parentNode !== null
    );
  }

  function decorateDiagram(diagram) {
    if (diagram.closest(`.${FRAME_CLASS}`)) {
      return;
    }

    const frame = document.createElement("div");
    frame.className = FRAME_CLASS;
    diagram.parentNode.insertBefore(frame, diagram);
    frame.appendChild(diagram);

    const button = document.createElement("button");
    button.type = "button";
    button.className = EXPAND_CLASS;
    button.setAttribute("aria-label", "Expand diagram");
    button.innerHTML = expandIcon;
    button.addEventListener("click", () => openOverlay(frame));
    frame.appendChild(button);
  }

  function decorateAll() {
    diagramTargets(document).forEach(decorateDiagram);
  }

  function scheduleDecorate() {
    window.requestAnimationFrame(() => {
      decorateAll();
      window.setTimeout(decorateAll, 250);
      window.setTimeout(decorateAll, 1000);
    });
  }

  if (typeof document$ !== "undefined" && document$.subscribe) {
    document$.subscribe(scheduleDecorate);
  } else if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", scheduleDecorate);
  } else {
    scheduleDecorate();
  }

  const observer = new MutationObserver(() => scheduleDecorate());
  observer.observe(document.documentElement, { childList: true, subtree: true });
  window.addEventListener("resize", resizeExpandedOverlay);
})();
