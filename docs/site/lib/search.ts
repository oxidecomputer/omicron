// The search modal, a custom element around markup that layout.tsx renders.
// buildSite compiles this file to search.js at the site root. Results come
// from Pagefind's JS API through search-api.ts and are rendered here, not by
// Pagefind's own UI, which clears the list on every keystroke and is hard to
// restyle.
//
// The input is an ARIA combobox: focus stays in it while the arrow keys move
// the selected option (aria-activedescendant) and Enter opens it. Class names
// in this file are picked up by Tailwind through an @source in site.css.

import { loadPagefind, search, type Pagefind, type Result } from './search-api.js'

// Selection styles only apply from 600px up, like the oxide.computer docs:
// on a phone there are no arrow keys and tapping is the way in.
const cls = {
  group: 'group/result',
  title:
    'text-mono-xs text-secondary bg-tertiary block truncate px-3 leading-6 600:group-has-[[aria-selected=true]]/result:bg-accent-inverse 600:group-has-[[aria-selected=true]]/result:text-inverse',
  option:
    'group/option border-secondary hover:bg-secondary block border-b px-4 py-3 600:aria-selected:bg-accent 600:aria-selected:outline 600:aria-selected:-outline-offset-1 600:aria-selected:outline-accent',
  heading: 'text-sans-md text-raise 600:group-aria-selected/option:text-accent mb-0.5 block',
  excerpt:
    'text-sans-md text-secondary 600:group-aria-selected/option:text-accent-tertiary line-clamp-2',
}

function el<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  className: string,
  props: Partial<HTMLElementTagNameMap[K]> = {},
) {
  return Object.assign(document.createElement(tag), { className }, props)
}

const isMac = /Mac|iPhone|iPad/.test(navigator.platform)

class OxideSearch extends HTMLElement {
  #pagefind?: Promise<Pagefind>
  #search = 0
  #options: HTMLAnchorElement[] = []
  #selected = -1
  #abort = new AbortController()

  get #dialog() {
    return this.querySelector('dialog')!
  }
  get #input() {
    return this.querySelector<HTMLInputElement>('[data-search-input]')!
  }
  get #list() {
    return this.querySelector<HTMLElement>('[data-search-list]')!
  }

  connectedCallback() {
    const signal = this.#abort.signal
    const dialog = this.#dialog
    const input = this.#input

    if (!isMac) {
      for (const k of this.querySelectorAll('[data-search-mod]')) k.textContent = 'Ctrl'
    }

    this.querySelector('[data-search-open]')!.addEventListener('click', () => this.open(), {
      signal,
    })
    this.querySelector('[data-search-close]')!.addEventListener('click', () => dialog.close(), {
      signal,
    })
    // A click on the backdrop lands on the dialog itself, since its content
    // fills it
    dialog.addEventListener('click', (e) => e.target === dialog && dialog.close(), { signal })
    dialog.addEventListener('close', () => this.#setExpanded(false), { signal })
    input.addEventListener('input', () => this.#update(), { signal })
    dialog.addEventListener('keydown', (e) => this.#onKey(e), { signal })
    // A result on the current page only scrolls to it, so close the dialog
    // to show it
    this.#list.addEventListener(
      'click',
      (e) => {
        const a = (e.target as Element).closest('a')
        if (a && a.pathname === location.pathname) dialog.close()
      },
      { signal },
    )
    document.addEventListener(
      'keydown',
      (e) => {
        if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
          e.preventDefault()
          if (dialog.open) dialog.close()
          else this.open()
        } else if (e.key === '/' && !dialog.open && !isEditable(e.target)) {
          e.preventDefault()
          this.open()
        }
      },
      { signal },
    )
  }

  disconnectedCallback() {
    this.#abort.abort()
  }

  open() {
    this.#dialog.showModal()
    this.#input.select()
    this.#setExpanded(this.#options.length > 0)
    this.#loadPagefind()
  }

  #loadPagefind() {
    this.#pagefind ??= loadPagefind(new URL('pagefind/', import.meta.url))
    return this.#pagefind
  }

  async #update() {
    const query = this.#input.value.trim()
    const id = ++this.#search
    const body = this.querySelector<HTMLElement>('[data-search-body]')!
    const summary = this.querySelector<HTMLElement>('[data-search-summary]')!

    if (!query) {
      body.hidden = true
      this.#render([])
      summary.textContent = ''
      return
    }

    // The previous results stay up until the new ones are ready. Only the
    // first search, while the index loads, waits long enough to say so.
    const slow = setTimeout(() => {
      if (id !== this.#search || !body.hidden) return
      body.hidden = false
      summary.textContent = 'Searching…'
    }, 300)
    this.#list.setAttribute('aria-busy', 'true')

    const shown = await search(await this.#loadPagefind(), query, 100)
    if (!shown || id !== this.#search) return
    clearTimeout(slow)

    this.#render(shown)
    body.hidden = false
    this.#list.removeAttribute('aria-busy')
    summary.textContent =
      shown.length === 0
        ? `No results for “${query}”`
        : `${shown.length} ${shown.length === 1 ? 'result' : 'results'} for “${query}”`
  }

  #render(results: Result[]) {
    // Each option is named by its section heading, or the page title for the
    // page's own row, and described by its excerpt, so a screen reader reads
    // "Scheme V0" and then the text around the match. The page title bar is
    // only read as the group's label.
    let n = 0
    const option = (href: string, labelId: string, excerpt: string) => {
      const id = `search-option-${n++}`
      // Not in the tab order: Tab moves between the input and the dialog's
      // buttons, and the arrow keys move through the results
      const a = el('a', cls.option, { id, href, tabIndex: -1 })
      a.setAttribute('role', 'option')
      a.setAttribute('aria-selected', 'false')
      a.setAttribute('aria-labelledby', labelId)
      a.setAttribute('aria-describedby', `${id}-excerpt`)
      const text = el('span', cls.excerpt, { id: `${id}-excerpt`, innerHTML: excerpt })
      return [a, text] as const
    }
    const groups = results.map((r, i) => {
      const titleId = `search-result-${i}`
      const title = el('div', cls.title, { id: titleId, textContent: r.title })
      title.setAttribute('aria-hidden', 'true')
      const group = el('div', cls.group, { role: 'group' } as Partial<HTMLDivElement>)
      group.setAttribute('aria-labelledby', titleId)
      const [page, pageText] = option(r.url, titleId, r.excerpt)
      page.append(pageText)
      group.append(title, page)
      for (const sub of r.sections) {
        const headingId = `search-option-${n}-heading`
        const [a, text] = option(sub.url, headingId, sub.excerpt)
        a.append(el('span', cls.heading, { id: headingId, textContent: sub.title }), text)
        group.append(a)
      }
      return group
    })
    this.#list.replaceChildren(...groups)
    this.#list.scrollTop = 0
    this.#options = [...this.#list.querySelectorAll<HTMLAnchorElement>('[role=option]')]
    this.#selected = -1
    this.#select(0)
    this.#setExpanded(this.#options.length > 0)
  }

  #select(i: number) {
    const input = this.#input
    this.#options[this.#selected]?.setAttribute('aria-selected', 'false')
    this.#selected = i
    const option = this.#options[i]
    if (!option) {
      input.removeAttribute('aria-activedescendant')
      return
    }
    option.setAttribute('aria-selected', 'true')
    input.setAttribute('aria-activedescendant', option.id)
    option.scrollIntoView({ block: 'nearest' })
    // Keep the page title above the page's first option in view too
    const title = option.previousElementSibling
    if (title && !title.matches('[role=option]')) title.scrollIntoView({ block: 'nearest' })
  }

  #setExpanded(expanded: boolean) {
    this.#input.setAttribute('aria-expanded', String(expanded && this.#dialog.open))
    this.querySelector('[data-search-open]')!.setAttribute(
      'aria-expanded',
      String(this.#dialog.open),
    )
  }

  #onKey(e: KeyboardEvent) {
    const n = this.#options.length
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault()
      if (n === 0) return
      const step = e.key === 'ArrowDown' ? 1 : -1
      this.#select((this.#selected + step + n) % n)
    } else if (e.key === 'Enter' && e.target === this.#input) {
      e.preventDefault()
      this.#options[this.#selected]?.click()
    } else if (
      !(e.target instanceof HTMLInputElement || e.target instanceof HTMLButtonElement) &&
      e.key.length === 1 &&
      !e.metaKey &&
      !e.ctrlKey
    ) {
      // Typing with focus elsewhere in the dialog goes to the input
      this.#input.focus()
    }
  }
}

function isEditable(target: EventTarget | null) {
  return (
    target instanceof HTMLElement &&
    (target.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(target.tagName))
  )
}

customElements.define('oxide-search', OxideSearch)
