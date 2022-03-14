<script>
import { h } from 'vue'

export default {
  functional: true,
  props: {
    id: String,
    level: {
      type: String,
      required: true,
      default: '3'
    }
  },
  render () {
    return h('h' + this.level, {}, this.$slots.default())
  }
}
</script>
