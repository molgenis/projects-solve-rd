<template>
    <div id="choice-container">
        <div>
            <h2>What would you like to find?</h2>
            <div class="flex">
                <div class="flex-child">
                    <div class="data-choice-option">
                        <svg
                            xmlns="http://www.w3.org/2000/svg"
                            class="heroicon"
                            fill="none"
                            width="24"
                            height="24"
                            viewBox="0 0 24 24"
                            stroke="currentColor"
                        >
                            <path
                                stroke-linecap="round"
                                stroke-linejoin="round"
                                stroke-width="2"
                                d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z"
                            />
                        </svg>
                        <input
                            id="df-option-subjects"
                            type="radio"
                            name="datafinder"
                            value="subjects"
                        />
                        <label for="df-option-subjects" class="option-label">
                            Subjects
                        </label>
                    </div>
                </div>
                <div class="flex-child">
                    <div class="data-choice-option">
                        <svg
                            xmlns="http://www.w3.org/2000/svg"
                            class="heroicon"
                            fill="none"
                            width="24"
                            height="24"
                            viewBox="0 0 24 24"
                            stroke="currentColor"
                        >
                            <path
                                stroke-linecap="round"
                                stroke-linejoin="round"
                                stroke-width="2"
                                d="M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z"
                            />
                        </svg>
                        <input
                            id="df-option-samples"
                            type="radio"
                            name="datafinder"
                            value="samples"
                        />
                        <label for="df-option-samples" class="option-label">
                            Samples
                        </label>
                    </div>
                </div>
                <div class="flex-child">
                    <div class="data-choice-option">
                        <svg
                            xmlns="http://www.w3.org/2000/svg"
                            class="heroicon"
                            fill="none"
                            width="24"
                            height="24"
                            viewBox="0 0 24 24"
                            stroke="currentColor"
                        >
                            <path
                                stroke-linecap="round"
                                stroke-linejoin="round"
                                stroke-width="2"
                                d="M8 16l2.879-2.879m0 0a3 3 0 104.243-4.242 3 3 0 00-4.243 4.242zM21 12a9 9 0 11-18 0 9 9 0 0118 0z"
                            />
                        </svg>
                        <input
                            id="df-option-experiments"
                            type="radio"
                            name="datafinder"
                            value="experiments"
                        />
                        <label for="df-option-experiments" class="option-label">
                            Experiments
                        </label>
                    </div>
                </div>
                <div class="flex-child">
                    <div class="data-choice-option">
                        <svg
                            xmlns="http://www.w3.org/2000/svg"
                            class="heroicon"
                            fill="none"
                            width="24"
                            height="24"
                            viewBox="0 0 24 24"
                            stroke="currentColor"
                        >
                            <path
                                stroke-linecap="round"
                                stroke-linejoin="round"
                                stroke-width="2"
                                d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
                            />
                        </svg>
                        <input
                            id="df-option-files"
                            type="radio"
                            name="datafinder"
                            value="files"
                        />
                        <label for="df-option-files" class="option-label">
                            Files
                        </label>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>

<script>
export default {}
</script>

<style lang="scss">
$grey-050: #f6f6f6;
$grey-500: #3f454b;
$grey-800: #353535;
$breakpoint: 1024px;

.data-choice-option {
    background-color: $grey-050;
    border-radius: 50%;
    width: 100px;
    box-sizing: content-box;
    padding: 2em;
    text-align: center;

    $icon-dim: 32px;

    .heroicon {
        width: $icon-dim;
        height: $icon-dim;
    }
}
</style>
