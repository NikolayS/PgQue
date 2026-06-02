// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

/*
 * The docs live in ../docs and are served under /docs/* (see content.config.ts).
 * Their Markdown uses relative links like `reference.md` and `tutorial.md#anchor`
 * so they stay valid on GitHub. Starlight's built-in relative-link rewriting does
 * not account for our `docs/` id prefix, so rewrite those links here instead.
 */
function rewriteDocsLinks() {
  const isRelativeMd = (href) =>
    typeof href === 'string' &&
    !/^[a-z]+:/i.test(href) &&
    !href.startsWith('/') &&
    !href.startsWith('#') &&
    /\.md(#.*)?$/.test(href);

  const toDocsPath = (href) => {
    const [path, hash = ''] = href.split('#');
    const name = path.replace(/^.*\//, '').replace(/\.md$/, '');
    const base = name === 'README' ? '/docs' : `/docs/${name}`;
    return hash ? `${base}#${hash}` : base;
  };

  return () => (tree) => {
    const visit = (node) => {
      if (node.tagName === 'a' && node.properties && isRelativeMd(node.properties.href)) {
        node.properties.href = toDocsPath(node.properties.href);
      }
      if (Array.isArray(node.children)) node.children.forEach(visit);
    };
    visit(tree);
  };
}

// https://astro.build/config
export default defineConfig({
  site: 'https://pgque.dev',
  markdown: {
    rehypePlugins: [rewriteDocsLinks()],
  },
  integrations: [
    starlight({
      title: 'PgQue',
      description:
        'Zero-bloat Postgres queue. PgQ repackaged for managed Postgres — one SQL file to install, pg_cron to tick.',
      logo: {
        light: './src/assets/logo-light.svg',
        dark: './src/assets/logo-dark.svg',
        replacesTitle: true,
      },
      social: {
        github: 'https://github.com/NikolayS/pgque',
      },
      components: {
        SocialIcons: './src/components/docs/SocialIcons.astro',
        ThemeSelect: './src/components/docs/ThemeSelect.astro',
      },
      editLink: {
        baseUrl: 'https://github.com/NikolayS/pgque/edit/main/docs/',
      },
      customCss: [
        '@fontsource-variable/jetbrains-mono',
        '@fontsource/saira-semi-condensed/500.css',
        '@fontsource/saira-semi-condensed/600.css',
        '@fontsource/saira-semi-condensed/700.css',
        '@fontsource/ibm-plex-sans/400.css',
        '@fontsource/ibm-plex-sans/500.css',
        '@fontsource/ibm-plex-sans/600.css',
        './src/styles/custom.css',
      ],
      head: [
        {
          tag: 'meta',
          attrs: { property: 'og:image', content: 'https://pgque.dev/og.png' },
        },
        {
          tag: 'meta',
          attrs: { name: 'twitter:card', content: 'summary_large_image' },
        },
        {
          /* Set data-theme-pref before paint so the toggle icon is correct (no FOUC). */
          tag: 'script',
          content:
            "(()=>{var s=localStorage.getItem('starlight-theme');document.documentElement.dataset.themePref=(s==='light'||s==='dark')?s:'auto';})();",
        },
      ],
      sidebar: [
        {
          label: 'Get started',
          items: [
            { label: 'Overview', slug: 'docs' },
            { label: 'Tutorial', slug: 'docs/tutorial' },
          ],
        },
        {
          label: 'Guides',
          items: [
            { label: 'Installation and operations', slug: 'docs/installation' },
            { label: 'Examples', slug: 'docs/examples' },
            { label: 'Monitoring and health', slug: 'docs/monitoring' },
          ],
        },
        {
          label: 'Reference',
          items: [{ label: 'Function reference', slug: 'docs/reference' }],
        },
        {
          label: 'Explanation',
          items: [
            { label: 'Latency and tick tuning', slug: 'docs/latency-and-tuning' },
            { label: 'Concepts and heritage', slug: 'docs/concepts' },
          ],
        },
      ],
    }),
  ],
});
