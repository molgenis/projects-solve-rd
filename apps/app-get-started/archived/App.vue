<template>
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
        <aside id="data-finder-sidebar" class="flex-child">
            <h2>Options</h2>
            <p>Apply filters and customize the table views.</p>
            <form @submit.prevent>
                <checkbox-group
                    id="freeze-filter"
                    title="Select a specific freeze"
                    name="filter__freezes"
                    v-bind:choices="rd3_freezes"
                    @change="update_freeze_filter"
                ></checkbox-group>
            </form>
        </aside>
        <div id="data-finder-main" class="flex-child">
            <h2>RD3 Freezes and Patches</h2>
            <div>
                <h3>Freeze 1</h3>
                <p>Freeze 1 data was released on 01 January 2019.</p>
                <link-section
                    id="freeze-1-original"
                    title="Original"
                    hlevel="4"
                >
                    <ul>
                        <li>
                            <molgenis-link
                                v-bind:host="host"
                                entity="rd3_freeze1_subject"
                                v-bind:columns="rd3_attrs.subject"
                                patch="freeze1_original"
                                label="Subject"
                            ></molgenis-link>
                        </li>
                        <li>
                            <molgenis-link
                                v-bind:host="host"
                                entity="rd3_freeze1_subjectinfo"
                                patch="freeze1_original"
                                label="Subject Information"
                            ></molgenis-link>
                        </li>
                        <li>
                            <molgenis-link
                                v-bind:host="host"
                                entity="rd3_freeze1_sample"
                                v-bind:columns="rd3_attrs.samples"
                                patch="freeze1_original"
                                label="Samples"
                            ></molgenis-link>
                        </li>
                        <li>
                            <molgenis-link
                                v-bind:host="host"
                                entity="rd3_freeze1_labinfo"
                                patch="freeze1_original"
                                label="Experiments"
                            ></molgenis-link>
                        </li>
                        <li>
                            <molgenis-link
                                v-bind:host="host"
                                entity="rd3_freeze1_file"
                                v-bind:columns="rd3_attrs.files"
                                patch="freeze1_original"
                                label="Files"
                            ></molgenis-link>
                        </li>
                    </ul>
                </link-section>
                <link-section
                    id="freeze-1-patch-1"
                    title="Patch 1"
                    hlevel="4"
                >
                    <ul>
                        <li>
                            <molgenis-link
                                v-bind:host="host"
                                entity="rd3_freeze1_subject"
                                v-bind:columns="rd3_attrs.subject"
                                patch="freeze1_patch1"
                                label="Subject"
                            ></molgenis-link>
                        </li>
                        <li>
                            <molgenis-link
                                v-bind:host="host"
                                entity="rd3_freeze1_subjectinfo"
                                patch="freeze1_patch1"
                                label="Subject Information"
                            ></molgenis-link>
                        </li>
                        <li>
                            <molgenis-link
                                v-bind:host="host"
                                entity="rd3_freeze1_sample"
                                v-bind:columns="rd3_attrs.samples"
                                patch="freeze1_patch1"
                                label="Samples"
                            ></molgenis-link>
                        </li>
                        <li>
                            <molgenis-link
                                v-bind:host="host"
                                entity="rd3_freeze1_labinfo"
                                patch="freeze1_patch1"
                                label="Experiments"
                            ></molgenis-link>
                        </li>
                        <li>
                            <molgenis-link
                                v-bind:host="host"
                                entity="rd3_freeze1_file"
                                v-bind:columns="rd3_attrs.files"
                                patch="freeze1_patch1"
                                label="Files"
                            ></molgenis-link>
                        </li>
                    </ul>
                </link-section>
            </div>
            <div>
                <h3>Freeze 2</h3>
                <p>Freeze 2 data was released on 15 March 2021.</p>
                <link-section
                    id="freeze-2-original"
                    title="Original"
                    hlevel="4"
                >
                    <ul>
                        <li>
                            <molgenis-link
                                v-bind:host="host"
                                entity="rd3_freeze2_subject"
                                v-bind:columns="rd3_attrs.subject"
                                patch="freeze2_original"
                                label="Subject"
                            ></molgenis-link>
                        </li>
                        <li>
                            <molgenis-link
                                v-bind:host="host"
                                entity="rd3_freeze2_subjectinfo"
                                patch="freeze2_original"
                                label="Subject Information"
                            ></molgenis-link>
                        </li>
                        <li>
                            <molgenis-link
                                v-bind:host="host"
                                entity="rd3_freeze2_sample"
                                v-bind:columns="rd3_attrs.samples"
                                patch="freeze2_original"
                                label="Samples"
                            ></molgenis-link>
                        </li>
                        <li>
                            <molgenis-link
                                v-bind:host="host"
                                entity="rd3_freeze2_labinfo"
                                patch="freeze2_original"
                                label="Experiments"
                            ></molgenis-link>
                        </li>
                        <li>
                            <molgenis-link
                                v-bind:host="host"
                                entity="rd3_freeze2_file"
                                v-bind:columns="rd3_attrs.files"
                                patch="freeze2_original"
                                label="Files"
                            ></molgenis-link>
                        </li>
                    </ul>
                </link-section>
            </div>
        </div>
    </div>
</template>

<script>
import LinkSection from './components/link_section.vue'
import RadioInput from './components/radio.vue'
import CheckboxGroup from './components/checkboxgroup.vue'
import MolgenisLink from './components/molgenis_link.vue'

// import rd3 attributes from file
import rd3Attributes from './data/rd3_entity_attributes.json'
import rd3 from './data/rd3_freezes.json'

export default {
  components: {
    'link-section': LinkSection,
    'radio-input': RadioInput,
    'molgenis-link': MolgenisLink,
    'checkbox-group': CheckboxGroup
  },
  data () {
    return {
      // host: "https://solve-rd.gcc.rug.nl",
      host: 'https://solve-rd-acc.gcc.rug.nl',
      filter_freezes: 'all',
      rd3_freezes: rd3.freeze_patches,
      rd3_links: rd3.tables,
      rd3_attrs: rd3Attributes
    }
  },
  methods: {
    update_freeze_filter (data) {
      console.log(data.id)
      // return (this.filter_freezes = data.value);
    }
  }
}
</script>

<style lang="scss">
$grey-050: #f6f6f6;
$grey-500: #3f454b;
$grey-800: #353535;
$breakpoint: 1024px;

h1,
h2 {
    color: $grey-800;
    margin: 0;
    margin-bottom: 12px;
}

fieldset {
    border: 0;
    padding: 0;

    legend {
        color: $grey-500;
        font-size: 11pt;
        text-transform: uppercase;
        letter-spacing: 1.2px;
        margin-bottom: 12px;
        font-weight: bold;
    }
}

#app {
    font-family: Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    color: $grey-500;
    width: 90%;
    margin: 60px auto;
    max-width: $breakpoint;
}

#data-finder-header {
    text-align: left;

    h1 {
        line-height: 1.4;
    }
    p {
        line-height: 1.8;
    }
}

.flex {
    display: flex;
    flex-direction: row;
    flex-wrap: wrap;

    .flex-child {
        box-sizing: border-box;
        padding: 1em 2em;
    }
}

#data-finder-sidebar {
    background-color: $grey-050;
}
</style>
