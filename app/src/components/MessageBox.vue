<template>
  <div class="message__box">
    <slot></slot>
  </div>
</template>

<script>

export default {
  name: 'message-box',
  props: {
    message: {
      type: String,
      required: false
    }
  }
}
</script>

<style lang="scss" scoped>
.message__box {
  background-color: #f6f6f6;
  box-sizing: padding-box;
  padding: 1em;
  margin: 12px 0;
  border-radius: 6px;
}
</style>
