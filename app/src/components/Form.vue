<template>
  <form class="form" @submit="(e) => e.preventDefault()">
    <legend class="form__title">
      <span>{{ title }}</span>
      <span class="form__description" v-if="description">{{ description }}</span>
    </legend>
    <div class="form__sections">
      <slot></slot>
    </div>
  </form>
</template>

<script>
export default {
  name: 'ui-form',
  props: {
    title: {
      type: String,
      required: true
    },
    description: {
      type: String,
      required: false
    }
  }
}
</script>

<style lang="scss" scoped>
.form {
  box-sizing: padding-box;
  background-color: #ffffff;
  padding: 2em;
  border-radius: 6px;
  width: 100%;
  margin: 0 auto;
  
  .form__title {
    display: block;
    margin-bottom: 16px;
    line-height: 1.6;
    .form__description {
      display: block;
      font-size: 11pt;
      line-height: 1.5;
    }
  }
}
</style>
