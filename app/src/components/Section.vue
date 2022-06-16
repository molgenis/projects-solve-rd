<template>
  <section :id="setidentifier + '-section'" class="app-section">
    <div class="app-section-content">
      <slot></slot>
    </div>
  </section>
</template>

<script>
export default {
  name: 'PageSection',
  props: ['id'],
  computed: {
    setidentifier () {
      return this.id.toLowerCase().replace(' ', '')
    }
  }
}
</script>

<style lang="scss">
.app-section {
  padding: 2em 1em;
  
  .app-section-content {
    width: 90%;
    margin: 0 auto;
    
    @media screen and (min-width: 972px) {
      max-width: 972px;
    }
  }
}
</style>
