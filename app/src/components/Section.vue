<template>
  <section :id="id" class="page__section">
    <div class="page__section__content">
      <slot></slot>
    </div>
  </section>
</template>

<script>
export default {
  name: 'PageSection',
  props: {
    id: {
      type: String,
      required: true
    }
  }
}
</script>

<style lang="scss">
.page__section {
  padding: 2em 1em;
  
  .page__section__content {
    width: 90%;
    margin: 0 auto;
    
    @media screen and (min-width: 972px) {
      max-width: 972px;
    }
  }
}
</style>
