<template>
  <div class="error-box">
    <slot></slot>
  </div>
</template>

<script>
export default {
  name: 'error-box'
}
</script>

<style lang="scss" scoped>
.error-box {
  background-color: hsl(1, 100%, 95%);
  color: hsl(10, 90%, 55%);
  box-sizing: padding-box;
  padding: 1.06em;
}
</style>
