<template>
  <div class="form__section">
    <slot></slot>
  </div>
</template>

<script>
export default {
  name: 'form-section'
}
</script>

<style lang="scss" scoped>
.form__section {
  box-sizing: border-box;
  padding: 1em 0;
  border-top: 1px solid #e6e6e6;
}
</style>
