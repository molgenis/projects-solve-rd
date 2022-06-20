<template>
  <img class="page-image" :src="src" />
</template>

<script>
export default {
  name: 'PageImage',
  props: {
    src: {
      type: String,
      required: true
    }
  }
}
</script>

<style lang="scss">
.page-image {
  display: block;
  margin: 1em auto;
  max-width: 90%;
  border-radius: 12px;
  
  @media screen and (min-width: 517px) {
    max-width: 525px;
  }
}
</style>
