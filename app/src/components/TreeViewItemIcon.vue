<template>
  <svg
    xmlns="http://www.w3.org/2000/svg"
    class="tree__item__icon"
    :data-icon-type="iconType"
    width="20"
    height="20"
    viewBox="0 0 20 20"
    fill="currentColor"
  >
    <path
      v-if="iconType === 'patient'"
      fill-rule="evenodd"
      d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-6-3a2 2 0 11-4 0 2 2 0 014 0zm-2 4a5 5 0 00-4.546 2.916A5.986 5.986 0 0010 16a5.986 5.986 0 004.546-2.084A5 5 0 0010 11z"
      clip-rule="evenodd"
    />
    <path
      v-else-if="iconType === 'folder-open'"
      fill-rule="evenodd"
      d="M4 4a2 2 0 00-2 2v8a2 2 0 002 2h12a2 2 0 002-2V8a2 2 0 00-2-2h-5L9 4H4zm4 6a1 1 0 100 2h4a1 1 0 100-2H8z"
    />
    <path
      v-else-if="iconType === 'folder-closed'"
      fill-rule="evenodd"
      d="M4 4a2 2 0 00-2 2v8a2 2 0 002 2h12a2 2 0 002-2V8a2 2 0 00-2-2h-5L9 4H4zm7 5a1 1 0 10-2 0v1H8a1 1 0 100 2h1v1a1 1 0 102 0v-1h1a1 1 0 100-2h-1V9z"
    />
    <path
      v-else-if="iconType === 'sample'"
      fill-rule="evenodd"
      d="M7 2a1 1 0 00-.707 1.707L7 4.414v3.758a1 1 0 01-.293.707l-4 4C.817 14.769 2.156 18 4.828 18h10.343c2.673 0 4.012-3.231 2.122-5.121l-4-4A1 1 0 0113 8.172V4.414l.707-.707A1 1 0 0013 2H7zm2 6.172V4h2v4.172a3 3 0 00.879 2.12l1.027 1.028a4 4 0 00-2.171.102l-.47.156a4 4 0 01-2.53 0l-.563-.187a1.993 1.993 0 00-.114-.035l1.063-1.063A3 3 0 009 8.172z"
      clip-rule="evenodd"
    />
    <path
      v-else
      fill-rule="evenodd"
      d="M4 4a2 2 0 012-2h4.586A2 2 0 0112 2.586L15.414 6A2 2 0 0116 7.414V16a2 2 0 01-2 2H6a2 2 0 01-2-2V4zm2 6a1 1 0 011-1h6a1 1 0 110 2H7a1 1 0 01-1-1zm1 3a1 1 0 100 2h6a1 1 0 100-2H7z"
      clip-rule="evenodd"
    />
  </svg>
</template>

<script>
export default {
  name: 'tree-item-icon',
  props: {
    iconType: {
      type: String,
      required: true
    }
  }
}
</script>

<style lang="scss" scoped>
.tree__item__icon {
  
  &[data-icon-type='folder-open'],
  &[data-icon-type='folder-closed'] {
   fill: #DEAF02;
  }
  
  &[data-icon-type='sample'] {
    fill: #478DAE;
  }
  
  &[data-icon-type='experiment'] {
    fill: #9e9e9e;
  }
}
</style>
