<template>
  <button :id="id" class="search__button">
    <svg
      xmlns="http://www.w3.org/2000/svg"
      class="button__icon"
      fill="none"
      width="16"
      height="16"
      viewBox="0 0 24 24"
      stroke="currentColor"
      stroke-width="2"
    >
    <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
    </svg>
    <span>{{ label }}</span>
  </button>
</template>

<script>
export default {
  name: 'search-button',
  props: {
    id: {
      type: String,
      required: true
    },
    label: {
      type: String,
      required: false,
      default: 'Search'
    }
  }
}

</script>

<style lang="scss" scoped>

$primary-color: #0d65fc;
$primary-color-inverse: hsl(218, 98%, 98%);

.search__button {
  display: flex;
  width: 100%;
  justify-content: center;
  align-items: center;
  background: none;
  border: none;
  padding: 6px 12px;
  background-color: $primary-color-inverse;
  border: 1px solid $primary-color;
  color: $primary-color;
  border-radius: 6px;
  
  .button__icon {
    margin-right: 6px;
  }
  
  &:hover, &:focus {
    outline: 1px solid;
  }
}
</style>
