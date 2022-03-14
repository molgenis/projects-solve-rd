<template>
    <section :id="id" :aria-labelledby="aria_labelledby">
        <anchored-heading :id="aria_labelledby" :level="hlevel">{{
            title
        }}</anchored-heading>
        <slot />
    </section>
</template>

<script>
import AnchoredHeading from './anchored_heading.vue'
export default {
  props: {
    id: String,
    title: String,
    hlevel: String,
    links: Object
  },
  components: {
    'anchored-heading': AnchoredHeading
  },
  computed: {
    aria_labelledby () {
      return (
        this.id.toLowerCase() +
                '-' +
                this.title.toLowerCase().replaceAll(' ', '-')
      )
    }
  }
}
</script>
