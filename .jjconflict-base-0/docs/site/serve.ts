// Serves dist/ at http://localhost:1414 for `npm run serve`. Pages live at
// directory URLs and search fetches its index, so file:// doesn't work.

import fs from 'node:fs'
import http from 'node:http'
import path from 'node:path'

const dist = path.join(import.meta.dirname, 'dist')
const port = 1414

const types: Record<string, string> = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css',
  '.js': 'text/javascript',
  '.json': 'application/json',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.woff2': 'font/woff2',
}

http
  .createServer((req, res) => {
    const pathname = decodeURIComponent(new URL(req.url ?? '/', 'http://localhost').pathname)
    let file = path.join(dist, pathname)
    if (!file.startsWith(dist)) return res.writeHead(403).end()
    if (fs.statSync(file, { throwIfNoEntry: false })?.isDirectory()) {
      // Like GitHub Pages, redirect foo to foo/ so relative links resolve
      if (!pathname.endsWith('/')) return res.writeHead(301, { Location: `${pathname}/` }).end()
      file = path.join(file, 'index.html')
    }
    fs.readFile(file, (err, data) => {
      if (err) return res.writeHead(404).end('Not found')
      res.writeHead(200, {
        'Content-Type': types[path.extname(file)] ?? 'application/octet-stream',
      })
      res.end(data)
    })
  })
  .listen(port, () => console.log(`Serving dist/ at http://localhost:${port}`))
