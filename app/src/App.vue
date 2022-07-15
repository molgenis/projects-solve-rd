<template>
  <div>
    <Navbar />
    <router-view></router-view>
  </div>
</template>

<script>
import Navbar from './components/tmpNavbar.vue'
export default {
  name: 'rd3-vue-app',
  components: { Navbar }
}
</script>

<style lang="scss">
.section-bg-plain {
  background-color: #ffffff;
}
</style>
