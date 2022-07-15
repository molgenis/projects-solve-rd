<template>
  <a :href="href" class="tree__item__link">
    <svg
      xmlns="http://www.w3.org/2000/svg"
      class="link__icon"
      fill="none"
      width="14"
      height="14"
      viewBox="0 0 24 24"
      stroke="currentColor"
      stroke-width="2">
      <path
        stroke-linecap="round"
        stroke-linejoin="round"
        d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
      />
    </svg>
    <span class="link__label">
      {{ hrefLabel ? hrefLabel : 'view record in database' }}
    </span>
  </a>
</template>

<script>
export default {
  name: 'tree-view-item-link',
  props: {
    href: {
      type: String
    },
    hrefLabel: {
      type: String
    }
  }
}
</script>

<style lang="scss" scoped>
.tree__item__link {
  .link__label {
    position: absolute;
    clip: rect(1px 1px 1px 1px); /* IE6, IE7 */
    clip: rect(1px, 1px, 1px, 1px);
    overflow: hidden;
    height: 1px;
    width: 1px;
    margin: -1px;
    white-space: nowrap;
  }
}
</style>
