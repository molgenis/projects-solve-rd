<template>
  <div>
    <header id="data-finder-header" class="header">
        <h1>Get started with the RD3 database</h1>
        <p>
            The RD3 database is structured into several primary tables: subjects
            and subject information, samples, experiment information, and file
            metadata. Since the initial release, data has been added to RD3 in
            batches &mdash; or freezes and patches. Use this page to find data
            across the data releases and apply initial data filters. Follow
            links generated on this page to the desired tables.
        </p>
    </header>
    <div id="data-finder-flex-container" class="flex">
        <aside id="data-finder-sidebar" class="flex-child sidebar ">
            <h2>Options</h2>
            <p>Apply filters and customize the table views.</p>
            <radio-buttons
                name="rd3menu"
                v-bind:options="menus"
                @change="getMenuSelection"
            ></radio-buttons>
            <div class="sidebar-submenu" v-if="activeMenu === 'Samples'">
                <h3>Sample specific filters</h3>
            </div>
            <div class="sidebar-submenu" v-else-if="activeMenu === 'Experiments'">
                <h3>Experiment specific filters</h3>
            </div>
            <div class="sidebar-submenu" v-else-if="activeMenu === 'Files'">
                <h3>Files specific filters</h3>
            </div>
            <div class="sidebar-submenu" v-else>
                <h3>Subject specific filters</h3>
            </div>
        </aside>
    </div>
    <div id="data-finder-main" class="flex-child"></div>
  </div>
</template>

<script>
import RadioButtons from './components/radio_buttons.vue'

export default {
  data () {
    return {
      menus: [
        { id: 'subject', label: 'Subjects', default: true },
        { id: 'samples', label: 'Samples' },
        { id: 'experiments', label: 'Experiments' },
        { id: 'files', label: 'Files' }
      ],
      activeMenu: 'Subjects'
    }
  },
  components: {
    'radio-buttons': RadioButtons
  },
  methods: {
    getMenuSelection (data) {
      this.activeMenu = data
      return data
    }
  }
}
</script>

<style lang="scss">
@import "styles/_variables.scss";
@import "styles/_base.scss";
@import "styles/index.scss";
</style>
